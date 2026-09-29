package com.github.xingjianluo.marketdatasnapshotservicejava.feed;

import com.github.xingjianluo.marketdatasnapshotservicejava.clock.Clock;
import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 进程内模拟行情源（无外部依赖）。
 *
 * <p>按固定间隔对配置的品种推送确定性随机游走行情，并且：
 * <ul>
 *   <li>每 11 条重发上一条（相同 eventId），用于观察 DUPLICATE；</li>
 *   <li>每 13 条发一条 eventTime 回拨 2s 的消息，用于观察 OUT_OF_ORDER；</li>
 *   <li>间隔可配到极小（如 0ms），配合小队列即可在本地复现 BACKPRESSURE。</li>
 * </ul>
 * 所有推送都走与外部 API 完全相同的 {@link IngestService#submit} 链路。
 */
@Component
@ConditionalOnProperty(prefix = "market.service", name = "feed-enabled", havingValue = "true")
public class SimulatedMarketFeed {

    private static final Logger log = LoggerFactory.getLogger(SimulatedMarketFeed.class);

    private final ServiceProperties properties;
    private final IngestService ingestService;
    private final Clock clock;
    private final List<String> symbols;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong sequence = new AtomicLong();
    private final Map<String, Long> lastPrice = new HashMap<>();
    private final Map<String, QuoteRequest> lastRequest = new HashMap<>();
    private Thread thread;

    public SimulatedMarketFeed(ServiceProperties properties, IngestService ingestService,
                               Clock clock) {
        this.properties = properties;
        this.ingestService = ingestService;
        this.clock = clock;
        this.symbols = Arrays.stream(properties.getFeedSymbols().split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();
        for (String s : symbols) {
            lastPrice.put(s, 10_000L + Math.abs(new Random(s.hashCode()).nextInt(90_000)));
        }
    }

    @PostConstruct
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        thread = new Thread(this::loop, "simulated-market-feed");
        thread.setDaemon(true);
        thread.start();
        log.info("SimulatedMarketFeed started: symbols={}, intervalMillis={}",
                symbols, properties.getFeedIntervalMillis());
    }

    private void loop() {
        while (running.get()) {
            for (String symbol : symbols) {
                if (!running.get()) {
                    break;
                }
                pushOne(symbol);
            }
            if (properties.getFeedIntervalMillis() > 0) {
                try {
                    Thread.sleep(properties.getFeedIntervalMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private void pushOne(String symbol) {
        long n = sequence.incrementAndGet();
        QuoteRequest previous = lastRequest.get(symbol);
        QuoteRequest request;
        if (previous != null && n % 11 == 0) {
            request = previous; // 重复消息
        } else {
            Random rnd = new Random(symbol.hashCode() * 31 + n);
            long nextPrice = Math.max(1, lastPrice.get(symbol) + rnd.nextInt(201) - 100);
            lastPrice.put(symbol, nextPrice);
            long eventTime = clock.nowMillis();
            if (n % 13 == 0) {
                eventTime -= 2_000; // 制造乱序（在 lag 窗口内）
            }
            String eventId = symbol + "-" + n;
            request = new QuoteRequest(eventId, symbol, eventTime, nextPrice,
                    1 + (n % 10));
            lastRequest.put(symbol, request);
        }
        IngestResult result = ingestService.submit(request);
        if (!result.accepted() && result.reason() != RejectReason.DUPLICATE
                && result.reason() != RejectReason.OUT_OF_ORDER) {
            log.warn("Simulated feed push not accepted: reason={}, detail={}",
                    result.reason(), result.detail());
        }
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
