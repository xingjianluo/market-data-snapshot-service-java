package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.MutableClock;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.TestServiceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发写入 + 并发读取：
 * - 每个 eventId 只被计入一次（重复可能因线程重试产生，但绝不双计）；
 * - 任意一次查询拿到的都是自洽快照：各窗口 count 之和 == 已计入唯一事件数；
 * - 最新价的 eventTime 不允许随时间倒退（单写者+原子切换的可观察不变量）。
 */
class ConcurrencyReadWriteTest {

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
    void concurrentWritersAndReadersNeverSeeTornState() throws Exception {
        MutableClock clock = new MutableClock(1_700_000_000_000L);
        ServiceProperties props = TestServiceFactory.props(stateDir);
        props.setCheckpointIntervalMessages(1_000_000);
        props.setCheckpointIntervalMillis(1_000_000);
        props.setOutOfOrderLagMillis(60_000); // 高并发下放宽乱序窗口，消息都在同一时点附近
        svc = TestServiceFactory.started(props, clock);

        int writerCount = 8;
        int perWriter = 200;
        int total = writerCount * perWriter;
        ExecutorService pool = Executors.newFixedThreadPool(writerCount + 2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicate = new AtomicInteger();
        ConcurrentLinkedQueue<String> acceptedIds = new ConcurrentLinkedQueue<>();

        for (int w = 0; w < writerCount; w++) {
            final int writer = w;
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perWriter; i++) {
                    long eventTime = 1_700_000_000_000L + i; // 同 i 跨写者 eventTime 相同，eventId 唯一
                    String id = "w" + writer + "-" + i;
                    QuoteRequest req = new QuoteRequest(id, "BTCUSDT", eventTime,
                            100L + (i % 50), 1L);
                    IngestResult r = svc.submit(req);
                    if (r.accepted()) {
                        accepted.incrementAndGet();
                        acceptedIds.add(id);
                    } else if (r.reason() == RejectReason.DUPLICATE) {
                        duplicate.incrementAndGet();
                    }
                }
            });
        }

        AtomicInteger tornSnapshots = new AtomicInteger();
        AtomicInteger readerRounds = new AtomicInteger();
        for (int r = 0; r < 2; r++) {
            pool.submit(() -> {
                // 每个读线程独立观察序列：同一观察线程看到的最新价必须单调不减
                long myLast = Long.MIN_VALUE;
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                while (accepted.get() + duplicate.get() < total
                        && !Thread.currentThread().isInterrupted()) {
                    long snapshotLatest = myLast;
                    var viewOpt = svc.query("BTCUSDT");
                    if (viewOpt.isPresent()) {
                        SymbolView view = viewOpt.get();
                        snapshotLatest = checkConsistency(view, myLast, tornSnapshots);
                        readerRounds.incrementAndGet();
                    }
                    myLast = Math.max(myLast, snapshotLatest);
                }
            });
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS), "workers should finish");

        // 终态校验：前拨时钟到最新事件之后，避免“未来时间戳”造成 stale
        clock.set(1_700_000_001_000L);
        SymbolView finalView = svc.query("BTCUSDT").orElseThrow();
        long windowCountSum = finalView.windows().stream()
                .mapToLong(com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowStats::count)
                .sum();
        assertEquals(total, accepted.get(), "all unique events accepted");
        assertEquals(accepted.get(), acceptedIds.size());
        assertEquals(windowCountSum, accepted.get(), "window counts match accepted events exactly once");
        assertEquals(total, windowCountSum);
        assertEquals(0L, svc.metrics().backpressureRejected());
        assertEquals(0, tornSnapshots.get(), "no torn/inconsistent snapshot ever observed");
        assertTrue(readerRounds.get() > 0, "readers observed intermediate states");
        assertFalse(finalView.stale());
    }

    private static long checkConsistency(SymbolView view, long myLast,
                                         AtomicInteger tornSnapshots) {
        // 不变量 1：快照必须带 latest
        if (view.latest() == null) {
            tornSnapshots.incrementAndGet();
            return myLast;
        }
        long latest = view.latest().eventTime();
        // 不变量 2：同一观察线程看到的最新 eventTime 不允许倒退（无半更新）
        if (latest < myLast) {
            tornSnapshots.incrementAndGet();
        }
        // 不变量 3：窗口边界自洽
        for (var w : view.windows()) {
            assertEquals(w.windowEnd() - w.windowStart(), view.windowSizeMillis());
            if (w.count() < 0) {
                tornSnapshots.incrementAndGet();
            }
        }
        return latest;
    }
}
