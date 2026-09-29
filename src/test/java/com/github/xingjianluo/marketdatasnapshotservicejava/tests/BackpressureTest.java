package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.OrderedProcessor;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.ServiceMetrics;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.QueueingIngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.AppendEventLog;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.InMemoryStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 背压语义测试：用极小队列 + 单条阻塞闸门，令队列填满后继续提交必须得到
 * BACKPRESSURE 显式拒绝；被拒绝数可从指标观察；已入队消息不丢；
 * 压力期间快照查询仍然可用；压力解除后 degraded 标志恢复。
 */
class BackpressureTest {

    /** 在处理指定 seq 前阻塞，直到测试放行。 */
    static final class GatedProcessor extends OrderedProcessor {
        volatile CountDownLatch gate;
        volatile long gatedSeq = -1;
        final AtomicInteger entered = new AtomicInteger();

        GatedProcessor(InMemoryStateStore store, ServiceProperties props, ServiceMetrics metrics) {
            super(store, props, metrics);
        }

        @Override
        public IngestionResult process(Quote quote, boolean recovery) {
            if (gate != null && quote.seqNum() == gatedSeq) {
                entered.incrementAndGet();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.process(quote, recovery);
        }
    }

    @Test
    void fullQueueRejectsExplicitlyWithoutDroppingEnqueuedData(@TempDir Path dir) throws Exception {
        ServiceProperties props = new ServiceProperties();
        props.setQueueCapacity(4);
        props.setPersistenceEnabled(false);
        props.setSimulatedFeedEnabled(false);

        InMemoryStateStore store = new InMemoryStateStore(1000, 60_000L);
        ServiceMetrics metrics = new ServiceMetrics();
        GatedProcessor processor = new GatedProcessor(store, props, metrics);
        AppendEventLog eventLog = new AppendEventLog(dir.toString(), "quotes",
                props.getLogMaxFileBytes(), false);
        QueueingIngestService service =
                new QueueingIngestService(props, store, processor, metrics, eventLog);
        metrics.bindObservables(service::queueSize, service::queueCapacity, service::isDegraded);

        long t = System.currentTimeMillis();
        // 第一条（seq=0）在处理器中被闸门挡住，后续入队把容量 4 的队列填满
        processor.gate = new CountDownLatch(1);
        processor.gatedSeq = 0;

        service.start();
        // seq=0 入队，被消费者取走到闸门内阻塞
        assertEquals(null, service.submit(quote(0, t)), "seq=0 should be enqueued");
        waitUntil(processor.entered::get, 1);

        // seq 1..4 填满队列（消费者被 seq=0 卡住，队列不再被取走）
        for (long seq = 1; seq <= 4; seq++) {
            assertEquals(null, service.submit(quote(seq, t + seq)),
                    "queue should accept until full");
        }
        waitUntil(service::queueSize, 4);

        // 队列已满：继续提交必须显式背压拒绝，而不是无界等待或静默丢弃
        int backpressured = 0;
        for (long seq = 5; seq <= 20; seq++) {
            IngestionResult r = service.submit(quote(seq, t + seq));
            assertNotNull(r, "full queue must return a rejection immediately");
            assertEquals(IngestionOutcome.REJECTED, r.outcome());
            assertEquals(RejectReason.BACKPRESSURE, r.reason());
            backpressured++;
        }
        assertEquals(16, backpressured);

        MetricsSnapshot during = service.getMetrics();
        assertEquals(16, during.rejectedBackpressure());
        assertEquals(4, during.queueSize());
        assertTrue(during.degraded(), "degraded must be observable during backpressure");

        // 压力下查询路径仍可响应（未知品种返回 404/null，是即时返回而非阻塞）
        assertEquals(null, service.getSnapshot("BPX"));

        // 放行：已入队的 5 条（seq 0..4）全部处理，不丢
        processor.gate.countDown();
        waitUntil(() -> (int) service.getMetrics().accepted(), 5);

        MetricsSnapshot after = service.getMetrics();
        assertEquals(5, after.accepted(), "all enqueued quotes must be processed, none dropped");
        assertEquals(16, after.rejectedBackpressure(), "rejection count must remain observable");
        assertEquals(0, after.queueSize());

        service.stop();
    }

    @Test
    void degradedFlagClearsAfterBackpressureSettles(@TempDir Path dir) throws Exception {
        ServiceProperties props = new ServiceProperties();
        props.setQueueCapacity(1);
        props.setPersistenceEnabled(false);
        InMemoryStateStore store = new InMemoryStateStore(1000, 60_000L);
        ServiceMetrics metrics = new ServiceMetrics();
        GatedProcessor processor = new GatedProcessor(store, props, metrics);
        AppendEventLog eventLog = new AppendEventLog(dir.toString(), "quotes",
                props.getLogMaxFileBytes(), false);
        QueueingIngestService service =
                new QueueingIngestService(props, store, processor, metrics, eventLog);
        metrics.bindObservables(service::queueSize, service::queueCapacity, service::isDegraded);

        long t = System.currentTimeMillis();
        processor.gate = new CountDownLatch(1);
        processor.gatedSeq = 0;
        service.start();
        // seq=0 入队并被消费者取走到闸门内阻塞，容量 1 的队列随即腾出一个槽
        assertEquals(null, service.submit(quote(0, t)));
        waitUntil(processor.entered::get, 1);
        // 再提交一条占满唯一槽位
        assertEquals(null, service.submit(quote(1, t + 1)));
        waitUntil(service::queueSize, 1);
        // 第三条必然背压
        assertEquals(RejectReason.BACKPRESSURE, service.submit(quote(9, t + 9)).reason());
        assertTrue(service.isDegraded());
        processor.gate.countDown();

        // 降级保持期过后应自动恢复
        Thread.sleep(QueueingIngestService.DEGRADED_HOLD_MILLIS + 200);
        assertFalse(service.isDegraded());
        service.stop();
    }

    private static Quote quote(long seq, long eventTime) {
        return new Quote("BP", "test", seq, eventTime, 10, 1, eventTime);
    }

    private static void waitUntil(java.util.function.IntSupplier condition, int expected)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsInt() == expected) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("timed out waiting for value " + expected
                + ", last=" + condition.getAsInt());
    }
}
