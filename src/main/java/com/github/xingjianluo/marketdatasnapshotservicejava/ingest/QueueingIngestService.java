package com.github.xingjianluo.marketdatasnapshotservicejava.ingest;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.OrderedProcessor;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.ServiceMetrics;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SnapshotView;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.AppendEventLog;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.InMemoryStateStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 面向调用方的写入/查询服务。
 *
 * <h3>背压</h3>
 * 入站通道是容量固定的 {@link LinkedBlockingQueue}：{@link #submit(Quote)} 使用非阻塞 offer，
 * 队列满时立即以 {@link RejectReason#BACKPRESSURE} 显式拒绝并计数——不会无界堆积，也不会悄悄丢数据
 * （入队成功的数据保证被处理）。发生背压拒绝期间 degraded=true，快照会标注数据可能滞后、可信度下降。
 *
 * <h3>顺序与一致</h3>
 * 单一消费者线程按 FIFO 取出并交给 {@link OrderedProcessor}，每个品种的复合状态更新在品种锁内原子完成；
 * 快照查询在同一把锁内复制不可变视图，因此并发读写下读到的总是某个一致时点。
 *
 * <h3>恢复</h3>
 * 启动时先在消费者线程启动前重放本地事件日志（只含历史上被接受的消息），
 * 处理器按 dedupKey 幂等，重复恢复（多次重放/日志与在线重叠）不会重复累计。
 */
public final class QueueingIngestService {

    private static final Logger log = LoggerFactory.getLogger(QueueingIngestService.class);

    /** 距上次背压拒绝多久以内仍视为降级（让短时尖峰在指标/快照上可见）。 */
    public static final long DEGRADED_HOLD_MILLIS = 2_000L;

    private final ServiceProperties properties;
    private final InMemoryStateStore store;
    private final OrderedProcessor processor;
    private final ServiceMetrics metrics;
    private final AppendEventLog eventLog;

    private final LinkedBlockingQueue<Quote> queue;
    /** 串行化所有状态写入：消费者线程与内联写入（模拟器/测试）共用，严格保持单写者。 */
    private final Object writeLock = new Object();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile long lastBackpressureRejectedAt = Long.MIN_VALUE;
    private Thread consumerThread;

    public QueueingIngestService(ServiceProperties properties,
                                 InMemoryStateStore store,
                                 OrderedProcessor processor,
                                 ServiceMetrics metrics,
                                 AppendEventLog eventLog) {
        this.properties = properties;
        this.store = store;
        this.processor = processor;
        this.metrics = metrics;
        this.eventLog = eventLog;
        this.queue = new LinkedBlockingQueue<>(properties.getQueueCapacity());
    }

    @PostConstruct
    public void start() {
        recover();
        running.set(true);
        consumerThread = new Thread(this::consumeLoop, "quote-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();
        log.info("ingest service started: queueCapacity={} disorderTolerance={}ms clockRollbackThreshold={}ms",
                properties.getQueueCapacity(),
                properties.getDisorderToleranceMillis(),
                properties.getClockRollbackThresholdMillis());
    }

    private void recover() {
        if (!properties.isPersistenceEnabled()) {
            log.info("persistence disabled, skip recovery");
            return;
        }
        List<Quote> recovered = eventLog.replay();
        long accepted = 0;
        long duplicates = 0;
        for (Quote q : recovered) {
            IngestionResult r = processor.process(q, true);
            if (r.outcome() == IngestionOutcome.ACCEPTED) {
                accepted++;
            } else if (r.outcome() == IngestionOutcome.DUPLICATE) {
                duplicates++;
            }
        }
        metrics.recordRecovered(accepted);
        log.info("recovery finished: replayed={} recoveredAccepted={} duplicatesSkipped={}",
                recovered.size(), accepted, duplicates);
    }

    private void consumeLoop() {
        while (running.get() || !queue.isEmpty()) {
            try {
                Quote quote = queue.poll(500, TimeUnit.MINUTES);
                if (quote == null) {
                    continue;
                }
                handle(quote);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                // 单条消息的意外异常不能杀死消费者；校验型失败本应已被处理器转为 REJECTED。
                log.error("unexpected error while consuming quote, message skipped", e);
            }
        }
        log.info("quote-consumer thread exited, drained=true");
    }

    private void handle(Quote quote) {
        IngestionResult result;
        synchronized (writeLock) {
            result = processor.process(quote, false);
            if (result.outcome() == IngestionOutcome.ACCEPTED && properties.isPersistenceEnabled()) {
                eventLog.append(quote);
            }
        }
        metrics.recordResult(quote.symbol(), result);
    }

    /**
     * 非阻塞提交：入队成功返回 null（最终异步处理）；队列已满返回 BACKPRESSURE 拒绝。
     * 背压在入队前判定，因此即使是重复消息，满队列时也会先报背压（通道级状态优先于内容判定）。
     */
    public IngestionResult submit(Quote quote) {
        metrics.recordReceived();
        if (!running.get()) {
            metrics.recordRejected(RejectReason.BACKPRESSURE);
            lastBackpressureRejectedAt = System.currentTimeMillis();
            return IngestionResult.rejected(RejectReason.BACKPRESSURE,
                    "service is shutting down, not accepting new quotes");
        }
        if (queue.offer(quote)) {
            return null;
        }
        metrics.recordRejected(RejectReason.BACKPRESSURE);
        lastBackpressureRejectedAt = System.currentTimeMillis();
        IngestionResult rejected = IngestionResult.rejected(RejectReason.BACKPRESSURE,
                String.format("bounded queue is full (capacity=%d, size=%d); backpressure, caller should retry",
                        properties.getQueueCapacity(), queue.size()));
        log.warn("quote rejected by backpressure: quote={} basis={}", quote, rejected.detail());
        return rejected;
    }

    /**
     * 在调用线程上同步处理（绕过队列）。仅供模拟行情源与测试使用：
     * 处理慢时以调用方线程的时间为代价自然限速，不会产生队列积压。
     */
    public IngestionResult processInline(Quote quote) {
        metrics.recordReceived();
        if (!running.get()) {
            metrics.recordRejected(RejectReason.BACKPRESSURE);
            lastBackpressureRejectedAt = System.currentTimeMillis();
            return IngestionResult.rejected(RejectReason.BACKPRESSURE,
                    "service is shutting down, not accepting new quotes");
        }
        IngestionResult result;
        synchronized (writeLock) {
            result = processor.process(quote, false);
            if (result.outcome() == IngestionOutcome.ACCEPTED && properties.isPersistenceEnabled()) {
                eventLog.append(quote);
            }
        }
        metrics.recordResult(quote.symbol(), result);
        return result;
    }

    public SnapshotView getSnapshot(String symbol) {
        return processor.snapshot(symbol, isDegraded());
    }

    public MetricsSnapshot getMetrics() {
        return metrics.snapshot();
    }

    public List<String> getSymbols() {
        return store.symbols();
    }

    public int queueSize() {
        return queue.size();
    }

    public int queueCapacity() {
        return properties.getQueueCapacity();
    }

    /** 近期（DEGRADED_HOLD_MILLIS 内）发生过背压拒绝即视为降级。 */
    public boolean isDegraded() {
        long last = lastBackpressureRejectedAt;
        return last != Long.MIN_VALUE
                && System.currentTimeMillis() - last <= DEGRADED_HOLD_MILLIS;
    }

    @PreDestroy
    public void stop() {
        log.info("ingest service stopping, queue remaining={}", queue.size());
        running.set(false);
        if (consumerThread != null) {
            consumerThread.interrupt();
            try {
                consumerThread.join(10_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        eventLog.close();
        log.info("ingest service stopped");
    }
}
