package com.github.xingjianluo.marketdatasnapshotservicejava.ingest;

import com.github.xingjianluo.marketdatasnapshotservicejava.clock.Clock;
import com.github.xingjianluo.marketdatasnapshotservicejava.dedup.DedupIndex;
import com.github.xingjianluo.marketdatasnapshotservicejava.dedup.DedupIndexRegistry;
import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.CheckpointState;
import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.CheckpointStore;
import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.DedupEntry;
import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.RecoveryService;
import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.WalWriter;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.QuoteStore;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 行情写入与查询主服务。
 *
 * <h3>写入链路（背压）</h3>
 * 提交线程做字段校验后，把请求放入<b>有界</b> {@link ArrayBlockingQueue}；
 * 队列满时最多等待 enqueueTimeoutMillis，仍无空位则以 {@code BACKPRESSURE}
 * 拒绝并计数——既不无界堆积也不静默丢弃。单个处理线程负责判定、去重、聚合、
 * WAL、检查点，因此状态更新天然串行。
 *
 * <h3>判定顺序与原因（互不静默丢弃）</h3>
 * <ol>
 *   <li>{@code INVALID_DATA}：字段缺失/非法（提交线程直接拒绝）；</li>
 *   <li>{@code CLOCK_ROLLBACK}：eventTime 相对服务端接收时间超出
 *       clockRollbackAgeMillis 的过去或未来范围（时钟不可信，与业务过期区分，
 *       且不占用 eventId，时钟恢复后可重试）；</li>
 *   <li>{@code DUPLICATE}：eventId 此前已计入（去重索引，跨重启有效）；</li>
 *   <li>{@code EXPIRED}：eventTime 落后最新事件超过 dataTtlMillis（数据过旧）；</li>
 *   <li>{@code OUT_OF_ORDER}：落后最新事件但在 outOfOrderLagMillis 内，
 *       按 eventTime 归位到对应窗口（计入），最新价指针不变；</li>
 *   <li>{@code ACCEPTED}：推进最新价（计入）；</li>
 *   <li>{@code PERSISTENCE_FAILURE}：WAL 落盘失败，内存不更新并显式拒绝。</li>
 * </ol>
 *
 * <h3>查询</h3>
 * 直接经 {@link QuoteStore} 的每品种 StampedLock 读取，不经过写入队列，
 * 压力下查询依然可用，且读到的是一致时点快照；stale 标志说明数据是否可信。
 */
@Service
public class IngestService {

    private static final Logger log = LoggerFactory.getLogger(IngestService.class);

    private final ServiceProperties properties;
    private final Clock clock;
    private final QuoteStore store;
    private final WalWriter walWriter;
    private final CheckpointStore checkpointStore;
    private final RecoveryService recoveryService;
    private final DedupIndexRegistry dedupRegistry;
    private final MetricsCollector metrics = new MetricsCollector();

    private final BlockingQueue<Task> queue;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread worker;

    private long messagesSinceCheckpoint;
    private long lastCheckpointMillis;
    /** 每条消息处理前额外停留，默认 0；供本地背压演示/测试把处理能力压到写入速率之下。 */
    private volatile long processDelayMillis = 0L;

    public void setProcessDelayMillis(long delayMillis) {
        this.processDelayMillis = Math.max(0, delayMillis);
    }

    public IngestService(ServiceProperties properties, Clock clock, QuoteStore store,
                         WalWriter walWriter, CheckpointStore checkpointStore,
                         RecoveryService recoveryService, DedupIndexRegistry dedupRegistry) {
        this.properties = properties;
        this.clock = clock;
        this.store = store;
        this.walWriter = walWriter;
        this.checkpointStore = checkpointStore;
        this.recoveryService = recoveryService;
        this.dedupRegistry = dedupRegistry;
        this.queue = new ArrayBlockingQueue<>(properties.getQueueCapacity());
        this.processDelayMillis = Math.max(0, properties.getProcessDelayMillis());
    }

    /** 启动：打开 WAL -> 恢复本地状态 -> 启动处理线程（保证恢复先于任何处理）。 */
    @PostConstruct
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        walWriter.open();
        long replayed = 0L;
        if (properties.isRecoveryEnabled()) {
            replayed = recoveryService.recover();
        } else {
            log.info("Recovery disabled by configuration");
        }
        this.lastCheckpointMillis = clock.nowMillis();
        worker = new Thread(this::processLoop, "ingest-worker");
        worker.setDaemon(true);
        worker.start();
        log.info("IngestService started: queueCapacity={}, walReplayed={}",
                properties.getQueueCapacity(), replayed);
    }

    public IngestResult submit(QuoteRequest request) {
        long receivedAt = clock.nowMillis();
        metrics.incrementReceived();
        if (!running.get()) {
            return reject(RejectReason.SERVICE_CLOSED, "service is not running", receivedAt, request);
        }
        String invalid = validate(request);
        if (invalid != null) {
            return reject(RejectReason.INVALID_DATA, invalid, receivedAt, request);
        }
        Quote quote = new Quote(request.eventId().trim(), request.symbol().trim(),
                request.eventTime(), request.price(), request.size());
        CompletableFuture<IngestResult> future = new CompletableFuture<>();
        boolean offered;
        try {
            offered = queue.offer(new Task(quote, receivedAt, future),
                    properties.getEnqueueTimeoutMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return reject(RejectReason.SERVICE_CLOSED, "interrupted while enqueue", receivedAt, request);
        }
        if (!offered) {
            metrics.recordOutcome(RejectReason.BACKPRESSURE);
            metrics.setDegraded(true, "ingest queue full: backpressure active");
            log.warn("BACKPRESSURE reject: eventId={}, symbol={}, queueDepth={}/{}, detail=offer timeout {}ms",
                    quote.eventId(), quote.symbol(), queue.size(), properties.getQueueCapacity(),
                    properties.getEnqueueTimeoutMillis());
            return IngestResult.rejected(RejectReason.BACKPRESSURE,
                    "queue full after " + properties.getEnqueueTimeoutMillis() + "ms", receivedAt);
        }
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return IngestResult.rejected(RejectReason.SERVICE_CLOSED,
                    "interrupted awaiting processing", receivedAt);
        } catch (Exception e) {
            return IngestResult.rejected(RejectReason.PERSISTENCE_FAILURE,
                    "processing failed: " + e.getMessage(), receivedAt);
        }
    }

    private String validate(QuoteRequest r) {
        if (r == null) {
            return "request body is required";
        }
        if (r.eventId() == null || r.eventId().isBlank()) {
            return "eventId is required";
        }
        if (r.symbol() == null || r.symbol().isBlank()) {
            return "symbol is required";
        }
        if (r.eventTime() == null) {
            return "eventTime is required";
        }
        if (r.price() == null) {
            return "price is required";
        }
        if (r.size() == null) {
            return "size is required";
        }
        if (r.eventTime() <= 0) {
            return "eventTime must be positive epoch millis";
        }
        if (r.price() <= 0) {
            return "price must be positive";
        }
        if (r.size() <= 0) {
            return "size must be positive";
        }
        return null;
    }

    private IngestResult reject(RejectReason reason, String detail, long receivedAt,
                                QuoteRequest request) {
        metrics.recordOutcome(reason);
        String eventId = request == null ? null : request.eventId();
        String symbol = request == null ? null : request.symbol();
        Long eventTime = request == null ? null : request.eventTime();
        log.info("REJECT reason={} eventId={} symbol={} eventTime={} serverTime={} detail={}",
                reason, eventId, symbol, eventTime, receivedAt, detail);
        return IngestResult.rejected(reason, detail, receivedAt);
    }

    private void processLoop() {
        while (running.get() || !queue.isEmpty()) {
            Task task;
            try {
                task = queue.poll(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (task == null) {
                continue;
            }
            try {
                task.future().complete(process(task.quote(), task.receivedAt()));
            } catch (Throwable t) {
                task.future().completeExceptionally(t);
            }
        }
        log.info("ingest-worker loop ended, drained=true, remaining={}", queue.size());
    }

    private IngestResult process(Quote quote, long receivedAt) {
        if (processDelayMillis > 0) {
            try {
                Thread.sleep(processDelayMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        String symbol = quote.symbol();
        DedupIndex dedup = dedupRegistry.indexFor(symbol);

        // 1) 时钟可信性先行：时间戳本身不可信（回拨/漂移过远）时不占用 eventId，
        //    时钟恢复后可用同一 eventId 重试。此判定与业务“过期”区分。
        long clockAge = receivedAt - quote.eventTime();
        if (clockAge > properties.getClockRollbackAgeMillis()) {
            return finish(false, RejectReason.CLOCK_ROLLBACK,
                    "eventTime is " + clockAge + "ms before server time, exceeds clock-rollback bound "
                            + properties.getClockRollbackAgeMillis() + "ms",
                    receivedAt, quote);
        }
        if (clockAge < -properties.getClockRollbackAgeMillis()) {
            return finish(false, RejectReason.CLOCK_ROLLBACK,
                    "eventTime is " + (-clockAge) + "ms ahead of server time, exceeds clock-drift bound "
                            + properties.getClockRollbackAgeMillis() + "ms",
                    receivedAt, quote);
        }

        // 2) 去重：仅对时间戳可信的消息登记；重复 = 同一 eventId 此前已计入（跨重启有效）
        if (!dedup.registerIfAbsent(quote.eventId(), quote.eventTime())) {
            return finish(false, RejectReason.DUPLICATE,
                    "eventId already processed: " + quote.eventId(), receivedAt, quote);
        }

        // 3) 业务时序判定（基于该品种已接受的最新 eventTime，与到达顺序解耦）
        long latest = store.latestEventTime(symbol);
        RejectReason outcome = RejectReason.ACCEPTED;
        String detail = "newest";
        if (latest != Long.MIN_VALUE && quote.eventTime() < latest) {
            long lag = latest - quote.eventTime();
            if (lag > properties.getDataTtlMillis()) {
                return finish(false, RejectReason.EXPIRED,
                        "eventTime is " + lag + "ms behind latest " + latest
                                + ", beyond ttl " + properties.getDataTtlMillis() + "ms",
                        receivedAt, quote);
            }
            if (lag > properties.getOutOfOrderLagMillis()) {
                return finish(false, RejectReason.EXPIRED,
                        "eventTime is " + lag + "ms behind latest " + latest
                                + ", beyond out-of-order window " + properties.getOutOfOrderLagMillis()
                                + "ms and counted as expired",
                        receivedAt, quote);
            }
            outcome = RejectReason.OUT_OF_ORDER;
            detail = "out-of-order within lag " + lag + "ms, placed into its eventTime window";
        }

        // 4) 先持久化再改内存：WAL 失败则内存不动，显式 PERSISTENCE_FAILURE
        try {
            walWriter.append(quote);
        } catch (RuntimeException e) {
            log.error("PERSISTENCE_FAILURE eventId={} symbol={}: {}",
                    quote.eventId(), symbol, e.getMessage());
            return finish(false, RejectReason.PERSISTENCE_FAILURE,
                    "WAL append failed: " + e.getMessage(), receivedAt, quote);
        }

        // 5) 内存聚合（单线程 + 每品种写锁，查询看到原子切换）
        boolean advanced = store.apply(quote);
        if (!advanced && outcome == RejectReason.ACCEPTED) {
            outcome = RejectReason.OUT_OF_ORDER;
            detail = "same/older eventTime tie, window updated, latest pointer unchanged";
        }
        messagesSinceCheckpoint++;
        maybeCheckpoint(receivedAt);
        return finish(true, outcome, detail, receivedAt, quote);
    }

    private IngestResult finish(boolean accepted, RejectReason reason, String detail,
                                long receivedAt, Quote quote) {
        metrics.recordOutcome(reason);
        if (reason == RejectReason.ACCEPTED || reason == RejectReason.OUT_OF_ORDER) {
            metrics.setDegraded(false, null);
            log.info("ACCEPT kind={} eventId={} symbol={} eventTime={} price={} size={} serverTime={} detail={}",
                    reason, quote.eventId(), quote.symbol(), quote.eventTime(),
                    quote.price(), quote.size(), receivedAt, detail);
        } else {
            log.info("REJECT reason={} eventId={} symbol={} eventTime={} serverTime={} detail={}",
                    reason, quote.eventId(), quote.symbol(), quote.eventTime(), receivedAt, detail);
        }
        return new IngestResult(accepted, reason, detail, receivedAt, quote);
    }

    private void maybeCheckpoint(long now) {
        boolean byCount = messagesSinceCheckpoint >= properties.getCheckpointIntervalMessages();
        boolean byTime = now - lastCheckpointMillis >= properties.getCheckpointIntervalMillis();
        if (!byCount && (!byTime || messagesSinceCheckpoint == 0)) {
            return;
        }
        checkpointNow(now);
    }

    /** 生成一致性检查点（最新价 + 窗口 + 去重索引）并轮转 WAL。 */
    public void checkpointNow() {
        checkpointNow(clock.nowMillis());
    }

    private synchronized void checkpointNow(long now) {
        Map<String, Quote> latestBySymbol = new HashMap<>();
        Map<String, List<com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowStats>>
                windowsBySymbol = new HashMap<>();
        Map<String, List<DedupEntry>> dedupBySymbol = new HashMap<>();
        for (String symbol : store.symbols()) {
            Quote latest = store.latestOf(symbol);
            if (latest != null) {
                latestBySymbol.put(symbol, latest);
            }
            windowsBySymbol.put(symbol, store.windowsOf(symbol));
            List<DedupEntry> entries = dedupRegistry.indexFor(symbol).entries();
            if (!entries.isEmpty()) {
                dedupBySymbol.put(symbol, entries);
            }
        }
        checkpointStore.save(new CheckpointState(now, latestBySymbol, windowsBySymbol, dedupBySymbol));
        walWriter.rotate();
        messagesSinceCheckpoint = 0;
        lastCheckpointMillis = now;
        metrics.incrementCheckpointCount(now);
    }

    public Optional<SymbolView> query(String symbol) {
        long now = clock.nowMillis();
        return store.view(symbol, now, properties.getStaleThresholdMillis());
    }

    public MetricsSnapshot metrics() {
        int depth = queue.size();
        long oldestAge = -1L;
        Task oldest = queue.peek();
        if (oldest != null) {
            oldestAge = clock.nowMillis() - oldest.receivedAt();
        }
        Map<String, Long> latestTimes = store.latestEventTimes();
        long now = clock.nowMillis();
        Map<String, Long> ages = new HashMap<>();
        for (Map.Entry<String, Long> e : latestTimes.entrySet()) {
            ages.put(e.getKey(), now - e.getValue());
        }
        return metrics.snapshot(depth, properties.getQueueCapacity(), oldestAge, latestTimes, ages);
    }

    /**
     * 测试/特殊场景使用：排空处理队列并关闭 WAL，但不做最终检查点（保留 WAL 供重放验证）。
     */
    public void shutdownWithoutFinalCheckpointForTest() {
        running.set(false);
        if (worker != null) {
            try {
                worker.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        walWriter.close();
    }

    @PreDestroy
    public void shutdown() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        log.info("IngestService shutting down, draining queue...");
        if (worker != null) {
            try {
                worker.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            checkpointNow();
        } catch (RuntimeException e) {
            log.warn("Final checkpoint failed", e);
        }
        walWriter.close();
        log.info("IngestService stopped, final checkpoint + WAL fsync done");
    }

    private record Task(Quote quote, long receivedAt, CompletableFuture<IngestResult> future) {
    }
}
