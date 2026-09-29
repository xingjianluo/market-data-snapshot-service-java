package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.OrderedProcessor;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.ServiceMetrics;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SnapshotView;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发读写一致性测试：
 * 多个写入线程 + 多个只读快照线程同时作用于同一品种。
 * 写路径在服务层共用一把写锁（此处直接模拟：写调用串行化），状态更新在品种锁内原子完成。
 * 校验：接受总数 == 唯一 (source,seqNum) 数量；任何时刻读到的快照内部自洽
 * （lastQuote 时间等于 lastUpdateEventTime、窗口 high>=low 等），无异常。
 */
class ConcurrencyConsistencyTest {

    private static final int WRITERS = 8;
    private static final int PER_WRITER = 500;
    private static final int READERS = 4;

    @Test
    void concurrentWritesAndSnapshotReadsStayConsistent() throws Exception {
        ServiceProperties props = new ServiceProperties();
        props.setDisorderToleranceMillis(5_000L);
        props.setClockRollbackThresholdMillis(60_000L);
        props.setFreshnessToleranceMillis(3_000L);
        props.setWindowLengthMillis(60_000L);
        props.setDedupCacheCapacity(100_000);

        InMemoryStateStore store = new InMemoryStateStore(100_000, 60_000L);
        OrderedProcessor processor = new OrderedProcessor(store, props, new ServiceMetrics());
        Object writeLock = new Object();

        AtomicBoolean stopReaders = new AtomicBoolean(false);
        AtomicInteger inconsistentSnapshots = new AtomicInteger();
        AtomicInteger readerErrors = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(WRITERS + READERS);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch writersDone = new CountDownLatch(WRITERS);

        for (int r = 0; r < READERS; r++) {
            pool.submit(() -> {
                try {
                    while (!stopReaders.get()) {
                        SnapshotView snap = processor.snapshot("CONC", false);
                        // null 也是合法观察：注册表项已建但首条数据尚未接受（HTTP 404 语义）
                        if (snap != null && !isInternallyConsistent(snap)) {
                            inconsistentSnapshots.incrementAndGet();
                        }
                    }
                } catch (RuntimeException e) {
                    readerErrors.incrementAndGet();
                }
            });
        }

        long base = 1_000_000L;
        // 全局递增的事件时间序号：保证多 writer 交错提交时事件时间仍然单调推进，
        // 排除乱序/回拨规则的干扰，专注检验并发下的计数与快照一致性。
        AtomicLongClock eventClock = new AtomicLongClock(base);
        AtomicLongClock seqClock = new AtomicLongClock(1);
        for (int w = 0; w < WRITERS; w++) {
            final int writerId = w;
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < PER_WRITER; i++) {
                        long eventTime = eventClock.next();
                        long seq = seqClock.next();
                        Quote quote = new Quote("CONC", "writer-" + writerId, seq,
                                eventTime, 100 + (i % 50), 1 + (i % 10), eventTime);
                        synchronized (writeLock) {
                            processor.process(quote, false);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    writersDone.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(writersDone.await(60, TimeUnit.SECONDS), "writers did not finish in time");
        stopReaders.set(true);
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(0, inconsistentSnapshots.get(), "observed internally inconsistent snapshot");
        assertEquals(0, readerErrors.get(), "reader threads saw exceptions");
        assertEquals((long) WRITERS * PER_WRITER, processor.acceptedCount("CONC"),
                "every unique quote must be accepted exactly once");
    }

    @Test
    void concurrentDuplicatesAreCountedOnce() throws Exception {
        ServiceProperties props = new ServiceProperties();
        InMemoryStateStore store = new InMemoryStateStore(10_000, 60_000L);
        OrderedProcessor processor = new OrderedProcessor(store, props, new ServiceMetrics());
        Object writeLock = new Object();

        int threads = 8;
        int copiesPerThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < copiesPerThread; i++) {
                        Quote q = new Quote("DUP", "src", i % 50, 1_000L + i, 10, 1, 1_000L + i);
                        synchronized (writeLock) {
                            processor.process(q, false);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
        // 唯一 dedupKey 只有 50 个，但 seq 相同而 eventTime 不同的迟到载荷不影响判重；
        // 接受数恰好为 50（其余全部 DUPLICATE 或因乱序规则在首次判重后被拒——判重优先，故仍是50）。
        assertEquals(50L, processor.acceptedCount("DUP"));
    }

    private static final class AtomicLongClock {
        private final java.util.concurrent.atomic.AtomicLong value;
        AtomicLongClock(long initial) { this.value = new java.util.concurrent.atomic.AtomicLong(initial); }
        long next() { return value.getAndIncrement(); }
    }

    private boolean isInternallyConsistent(SnapshotView snap) {
        if (snap.lastQuote() == null) {
            return false;
        }
        if (snap.lastQuote().eventTimeMillis() != snap.lastUpdateEventTimeMillis()) {
            return false;
        }
        if (snap.window() != null) {
            return snap.window().high() >= snap.window().low()
                    && snap.window().tickCount() > 0
                    && snap.window().vwap() > 0;
        }
        return true;
    }
}
