package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.OrderedProcessor;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.ServiceMetrics;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.QueueingIngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SnapshotView;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.AppendEventLog;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.InMemoryStateStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 进程重启恢复测试：
 * <ol>
 *   <li>第一个"进程"接受若干报价（含一条容忍窗口内的乱序），落盘后关闭；</li>
 *   <li>第二个"进程"指向同一数据目录启动，快照与聚合应恢复为关闭前的状态；</li>
 *   <li>再次重启（同一日志重复恢复）计数不得翻倍——重复恢复幂等；</li>
 *   <li>恢复后重放历史中的消息应判 DUPLICATE，新消息继续正常接受。</li>
 * </ol>
 */
class RecoveryRestartTest {

    private static final String SYM = "RST";

    private static ServiceProperties props(Path dir) {
        ServiceProperties p = new ServiceProperties();
        p.setQueueCapacity(64);
        p.setPersistenceEnabled(true);
        p.setDataDir(dir.toString());
        p.setSimulatedFeedEnabled(false);
        p.setDisorderToleranceMillis(5_000L);
        p.setClockRollbackThresholdMillis(60_000L);
        p.setWindowLengthMillis(60_000L);
        return p;
    }

    private static final class Context {
        final InMemoryStateStore store;
        final ServiceMetrics metrics;
        final OrderedProcessor processor;
        final AppendEventLog eventLog;
        final QueueingIngestService service;

        Context(Path dir) {
            ServiceProperties p = props(dir);
            store = new InMemoryStateStore(10_000, p.getWindowLengthMillis());
            metrics = new ServiceMetrics();
            processor = new OrderedProcessor(store, p, metrics);
            eventLog = new AppendEventLog(p.getDataDir(), p.getLogFilePrefix(),
                    p.getLogMaxFileBytes(), p.isLogFsync());
            service = new QueueingIngestService(p, store, processor, metrics, eventLog);
            metrics.bindObservables(service::queueSize, service::queueCapacity, service::isDegraded);
        }

        void start() { service.start(); }
        void stop() { service.stop(); }
    }

    private static Quote quote(long seq, long eventTime, double price) {
        return new Quote(SYM, "src", seq, eventTime, price, 1.0d, eventTime);
    }

    @Test
    void stateRecoversAfterRestartAndRepeatedRecoveryIsIdempotent(@TempDir Path dir) throws Exception {
        long base = 10_000_000L;

        // ---- 第一个进程：写入 5 条，其中 seq=4 是容忍窗口内乱序 ----
        Context first = new Context(dir);
        first.start();
        IngestionResult[] writes = {
                first.service.processInline(quote(1, base, 100)),
                first.service.processInline(quote(2, base + 1_000, 101)),
                first.service.processInline(quote(3, base + 2_000, 102)),
                first.service.processInline(quote(4, base - 2_000, 99)),   // 乱序归位
                first.service.processInline(quote(5, base + 3_000, 103)),
                first.service.processInline(quote(2, base + 1_000, 101)), // 重复，不落盘
        };
        for (int i = 0; i < 5; i++) {
            assertEquals(IngestionOutcome.ACCEPTED, writes[i].outcome(), "write " + i);
        }
        assertEquals(IngestionOutcome.DUPLICATE, writes[5].outcome());
        assertEquals(5L, first.service.getSnapshot(SYM).lastQuote() == null ? -1
                : first.processor.acceptedCount(SYM));
        SnapshotView before = first.service.getSnapshot(SYM);
        assertNotNull(before.window());
        assertEquals(5, before.window().tickCount());
        first.stop();

        // ---- 第二个进程：从同目录恢复 ----
        Context second = new Context(dir);
        second.start();
        SnapshotView restored = second.service.getSnapshot(SYM);
        assertNotNull(restored, "snapshot must be available immediately after recovery");
        assertEquals(base + 3_000, restored.lastUpdateEventTimeMillis(),
                "max event time must recover");
        assertEquals(base + 3_000, restored.lastQuote().eventTimeMillis(),
                "latest quote must recover by event time");
        assertEquals(103.0, restored.lastQuote().price(), 0);
        assertNotNull(restored.window());
        assertEquals(5, restored.window().tickCount(), "window aggregate must recover");
        assertEquals(99.0, restored.window().low(), 0);
        assertEquals(103.0, restored.window().high(), 0);
        assertEquals(5L, second.processor.acceptedCount(SYM),
                "recovered accepted count must equal pre-shutdown count, not doubled");
        MetricsSnapshot metricsAfterFirstRecovery = second.metrics.snapshot();
        assertEquals(5L, metricsAfterFirstRecovery.recovered());

        // 历史消息在恢复后的进程中重放：必须判重
        IngestionResult replayDup = second.service.processInline(quote(3, base + 2_000, 102));
        assertEquals(IngestionOutcome.DUPLICATE, replayDup.outcome());
        assertEquals(5L, second.processor.acceptedCount(SYM));

        // 新消息可继续接受
        IngestionResult fresh = second.service.processInline(quote(6, base + 4_000, 104));
        assertEquals(IngestionOutcome.ACCEPTED, fresh.outcome());
        assertEquals(6L, second.processor.acceptedCount(SYM));
        second.stop();

        // ---- 第三个进程：同一批历史被再次恢复，计数仍不翻倍 ----
        Context third = new Context(dir);
        third.start();
        assertEquals(6L, third.processor.acceptedCount(SYM),
                "second restart must not double-count history");
        SnapshotView twice = third.service.getSnapshot(SYM);
        assertEquals(6, twice.window().tickCount());
        assertEquals(base + 4_000, twice.lastUpdateEventTimeMillis());
        assertEquals(6L, third.metrics.snapshot().recovered());
        third.stop();

        // 日志中应当恰好有 6 行（每条被接受的消息一行，重复/拒绝不落盘）
        try (var lines = java.nio.file.Files.lines(dir.resolve("quotes.log"))) {
            long linesCount = lines.count();
            // 若发生过滚动，部分历史可能在 .1；两文件合计恰好 6
            long rolled = java.nio.file.Files.exists(dir.resolve("quotes.log.1"))
                    ? java.nio.file.Files.readAllLines(dir.resolve("quotes.log.1")).size() : 0L;
            assertTrue(linesCount + rolled == 6,
                    "expected exactly 6 durable accepted records, got active=" + linesCount
                            + " rolled=" + rolled);
        }
    }

    @Test
    void rejectedMessagesAreNotDurableAndDoNotReappearAfterRestart(@TempDir Path dir) throws Exception {
        Context first = new Context(dir);
        first.start();
        long base = 20_000_000L;
        first.service.processInline(quote(1, base, 100));
        // 过期与时钟回拨：拒绝，不落盘
        IngestionResult stale = first.service.processInline(quote(2, base - 8_000, 100));
        IngestionResult rollback = first.service.processInline(quote(3, base - 70_000, 100));
        assertEquals(IngestionOutcome.REJECTED, stale.outcome());
        assertEquals(IngestionOutcome.REJECTED, rollback.outcome());
        first.stop();

        Context second = new Context(dir);
        second.start();
        assertEquals(1L, second.processor.acceptedCount(SYM),
                "only the accepted quote is durable; rejects must not reappear");
        second.stop();
    }
}
