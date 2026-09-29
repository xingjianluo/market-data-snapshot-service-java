package com.github.xingjianluo.marketdatasnapshotservicejava.feed;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.QueueingIngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 进程内模拟行情源：无需任何外部连接即可让服务持续收到数据。
 *
 * <ul>
 *   <li>固定节拍为若干品种生成价格随机游走的报价，通过 {@code processInline} 同步送入，
 *       处理慢时以节拍线程为代价自然限速，不制造无意义积压；</li>
 *   <li>周期性注入可解释样本：
 *     <ul>
 *       <li>每 ~11 拍重发上一条报价（重复）；</li>
 *       <li>每 ~17 拍补发一条落后于容忍窗口但未到回拨阈值的报价（过期 STALE）；</li>
 *       <li>每 ~23 拍补发一条轻微乱序（容忍窗口内，应被归位接受）；</li>
 *       <li>时钟回拨样本通过 {@link #injectClockRollback(String)} 显式触发。</li>
 *     </ul>
 *   </li>
 * </ul>
 * 判定结果均由处理器打印输入与依据；本类也记录注入动作，方便对照日志验证。
 */
@Component
public class SimulatedMarketDataSource {

    private static final Logger log = LoggerFactory.getLogger(SimulatedMarketDataSource.class);
    private static final String[] SYMBOLS = {"SIM.A", "SIM.B", "SIM.C"};

    private final QueueingIngestService ingestService;
    private final ServiceProperties properties;
    private final Random random = new Random(42);
    private final Map<String, AtomicLong> seqBySymbol = new ConcurrentHashMap<>();
    private final Map<String, Double> priceBySymbol = new ConcurrentHashMap<>();
    private final Map<String, Quote> lastBySymbol = new ConcurrentHashMap<>();
    private final AtomicLong tick = new AtomicLong();

    private ScheduledExecutorService scheduler;

    public SimulatedMarketDataSource(QueueingIngestService ingestService,
                                     ServiceProperties properties) {
        this.ingestService = ingestService;
        this.properties = properties;
        for (String s : SYMBOLS) {
            seqBySymbol.put(s, new AtomicLong(1));
            priceBySymbol.put(s, 100.0d);
        }
    }

    @PostConstruct
    public void start() {
        if (!properties.isSimulatedFeedEnabled()) {
            log.info("simulated feed disabled");
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "simulated-feed");
            t.setDaemon(true);
            return t;
        });
        long interval = properties.getSimulatedFeedIntervalMillis();
        scheduler.scheduleAtFixedRate(this::safeEmit, interval, interval, TimeUnit.MILLISECONDS);
        log.info("simulated feed started: symbols={} intervalMs={}", String.join(",", SYMBOLS), interval);
    }

    private void safeEmit() {
        try {
            emitOnce();
        } catch (RuntimeException e) {
            log.warn("simulated feed tick failed: {}", e.getMessage());
        }
    }

    void emitOnce() {
        long now = System.currentTimeMillis();
        long n = tick.incrementAndGet();
        for (String symbol : SYMBOLS) {
            if (n % 11 == 0) {
                Quote last = lastBySymbol.get(symbol);
                if (last != null) {
                    log.info("sim-feed inject DUPLICATE sample: {}", last);
                    report(symbol, ingestService.processInline(last));
                    continue;
                }
            }
            if (n % 17 == 0) {
                Quote stale = buildQuote(symbol,
                        now - properties.getDisorderToleranceMillis() - 1_000L);
                log.info("sim-feed inject STALE (beyond disorder tolerance) sample: {}", stale);
                report(symbol, ingestService.processInline(stale));
                continue;
            }
            if (n % 23 == 0) {
                Quote reordered = buildQuote(symbol,
                        now - Math.max(1L, properties.getDisorderToleranceMillis() / 2));
                log.info("sim-feed inject OUT-OF-ORDER within tolerance sample: {}", reordered);
                report(symbol, ingestService.processInline(reordered));
                continue;
            }
            Quote quote = buildQuote(symbol, now);
            lastBySymbol.put(symbol, quote);
            report(symbol, ingestService.processInline(quote));
        }
    }

    /**
     * 显式注入一条严重时钟回拨样本（事件时间落后超过回拨阈值），供 HTTP/测试验证 CLOCK_ROLLBACK。
     */
    public Quote injectClockRollback(String symbol) {
        // buildQuote 内部对任意品种都能惰性分配序列号
        Quote quote = buildQuote(symbol,
                System.currentTimeMillis() - properties.getClockRollbackThresholdMillis() - 10_000L);
        log.info("sim-feed inject CLOCK-ROLLBACK sample per request: {}", quote);
        report(symbol, ingestService.processInline(quote));
        return quote;
    }

    /** 显式注入一条非法数据样本，供验证 INVALID_DATA 与其他原因的区分。 */
    public Quote injectInvalid(String symbol) {
        Quote quote = new Quote(symbol, "sim-invalid",
                seqBySymbol.computeIfAbsent(symbol, k -> new AtomicLong(1)).getAndIncrement(),
                System.currentTimeMillis(), -1.0d, 10.0d, System.currentTimeMillis());
        log.info("sim-feed inject INVALID sample: {}", quote);
        report(symbol, ingestService.processInline(quote));
        return quote;
    }

    private Quote buildQuote(String symbol, long eventTimeMillis) {
        long seq = seqBySymbol.computeIfAbsent(symbol, k -> new AtomicLong(1)).getAndIncrement();
        double price = priceBySymbol.compute(symbol, (k, prev) -> {
            double base = prev == null ? 100.0d : prev;
            double next = base + (random.nextDouble() - 0.5d) * 0.5d;
            return Math.max(0.01d, next);
        });
        double size = 1.0d + random.nextInt(10);
        return new Quote(symbol, "sim", seq, eventTimeMillis,
                Math.round(price * 100.0d) / 100.0d, size, System.currentTimeMillis());
    }

    private void report(String symbol, IngestionResult result) {
        if (result != null && result.reason() == RejectReason.BACKPRESSURE) {
            log.warn("sim-feed backpressured for {}: {}", symbol, result.detail());
        }
    }

    @PreDestroy
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            log.info("simulated feed stopped");
        }
    }
}
