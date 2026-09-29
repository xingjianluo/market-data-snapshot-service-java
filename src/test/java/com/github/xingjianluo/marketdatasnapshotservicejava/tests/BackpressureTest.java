package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.MutableClock;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.TestServiceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 背压语义：处理能力跟不上写入速率时，队列有界 -> 超时以 BACKPRESSURE 显式拒绝，
 * 指标暴露 queueDepth/oldestQueuedAge/backpressureRejected/degraded；
 * 被接受的消息一条不丢，查询路径不经队列、压力下始终可用。
 *
 * <p>单提交线程因同步等待结果会与处理线程串行，故用多写线程同时涌入复现真实过载。
 */
class BackpressureTest {

    @TempDir
    Path stateDir;
    private IngestService svc;

    @AfterEach
    void tearDown() {
        if (svc != null) {
            svc.shutdown();
        }
    }

    @Test
    void boundedQueueRejectsWithBackpressureAndStaysQueryable() throws Exception {
        MutableClock clock = new MutableClock(1_700_000_000_000L);
        ServiceProperties props = TestServiceFactory.props(stateDir);
        props.setQueueCapacity(4);
        props.setEnqueueTimeoutMillis(10);
        props.setCheckpointIntervalMessages(1_000_000);
        props.setCheckpointIntervalMillis(1_000_000);
        svc = TestServiceFactory.started(props, clock);
        svc.setProcessDelayMillis(2); // 压低处理能力，使多写线程涌入时必然积压

        int writers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong accepted = new AtomicLong();
        AtomicLong backpressure = new AtomicLong();

        for (int w = 0; w < writers; w++) {
            final int writer = w;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (long i = 0; i < 500; i++) {
                    IngestResult r = svc.submit(new QuoteRequest(
                            "bp-" + writer + "-" + i, "BTCUSDT",
                            1_700_000_000_000L + i, 100L, 1L));
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                    } else if (r.reason() == RejectReason.BACKPRESSURE) {
                        backpressure.incrementAndGet();
                    }
                }
            });
        }

        // 观察线程：压力期间校验队列有界、查询始终可用，并抓取下压力时刻指标
        AtomicLong observedMaxDepth = new AtomicLong();
        AtomicLong observedOldestAge = new AtomicLong();
        final MetricsSnapshot[] underPressure = new MetricsSnapshot[1];
        pool.submit(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            while (!Thread.currentThread().isInterrupted()) {
                MetricsSnapshot m = svc.metrics();
                observedMaxDepth.set(Math.max(observedMaxDepth.get(), m.queueDepth()));
                observedOldestAge.set(Math.max(observedOldestAge.get(), m.oldestQueuedAgeMillis()));
                if (m.backpressureRejected() > 0 && underPressure[0] == null) {
                    underPressure[0] = m;
                }
                assertTrue(m.queueDepth() <= m.queueCapacity(), "queue must stay bounded");
                if (accepted.get() > 0) {
                    assertTrue(svc.query("BTCUSDT").isPresent(),
                            "query must remain available under pressure");
                }
                if (backpressure.get() >= 50 && accepted.get() > 0) {
                    return;
                }
            }
        });

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "writers should finish");

        assertTrue(backpressure.get() >= 50,
                "expected >=50 backpressure rejections, got " + backpressure.get());
        assertTrue(observedMaxDepth.get() > 0, "queue buildup should be observable");
        assertTrue(underPressure[0] != null, "a metrics snapshot during pressure should exist");
        assertTrue(underPressure[0].degraded(), "degraded flag set under backpressure");
        assertEquals(4, underPressure[0].queueCapacity());

        // 排空后所有“已接受”消息一条不丢
        long expectedAccepted = accepted.get();
        drain(expectedAccepted);

        // 恢复：排空后继续写入恢复正常，降级解除
        long recovered = 0;
        for (int k = 0; k < 20; k++) {
            clock.set(1_700_000_001_000L + k);
            IngestResult r = svc.submit(new QuoteRequest("after-" + k, "ETHUSDT",
                    clock.nowMillis(), 100L, 1L));
            if (r.accepted()) {
                recovered++;
            }
        }
        assertTrue(recovered >= 15, "service should recover after drain, got " + recovered);
        assertTrue(svc.query("ETHUSDT").isPresent());
    }

    private void drain(long expectedAccepted) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            MetricsSnapshot m = svc.metrics();
            SymbolView view = svc.query("BTCUSDT").orElse(null);
            long counted = view == null ? 0 : view.windows().stream()
                    .mapToLong(w -> w.count()).sum();
            if (m.queueDepth() == 0 && counted == expectedAccepted) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("queue did not fully drain or accepted messages were lost");
    }
}
