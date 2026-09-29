package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.OrderedProcessor;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.ServiceMetrics;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SnapshotView;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.InMemoryStateStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderedProcessorTest {

    private static final String SYM = "600000";
    private OrderedProcessor processor;

    private static ServiceProperties props() {
        ServiceProperties p = new ServiceProperties();
        p.setDisorderToleranceMillis(5_000L);
        p.setClockRollbackThresholdMillis(60_000L);
        p.setFreshnessToleranceMillis(3_000L);
        p.setWindowLengthMillis(60_000L);
        p.setDedupCacheCapacity(1000);
        return p;
    }

    @BeforeEach
    void setUp() {
        InMemoryStateStore store = new InMemoryStateStore(1000, 60_000L);
        processor = new OrderedProcessor(store, props(), new ServiceMetrics());
    }

    private Quote quote(long seq, long eventTime, double price, double size) {
        return new Quote(SYM, "src", seq, eventTime, price, size, eventTime);
    }

    @Test
    void firstQuoteIsAccepted() {
        IngestionResult r = processor.process(quote(1, 1_000_000L, 10, 1), false);
        assertEquals(IngestionOutcome.ACCEPTED, r.outcome());
    }

    @Test
    void duplicateSameSourceAndSeqIsNotCountedTwice() {
        long t = 1_000_000L;
        assertEquals(IngestionOutcome.ACCEPTED, processor.process(quote(1, t, 10, 1), false).outcome());
        // 完全重复
        assertEquals(IngestionOutcome.DUPLICATE, processor.process(quote(1, t, 10, 1), false).outcome());
        // 即使内容不同（迟到的重传载荷），同一 source+seqNum 仍判重
        assertEquals(IngestionOutcome.DUPLICATE, processor.process(quote(1, t, 99, 9), false).outcome());
        assertEquals(1L, processor.acceptedCount(SYM));
    }

    @Test
    void sameSeqFromDifferentSourceIsNotDuplicate() {
        long t = 1_000_000L;
        assertEquals(IngestionOutcome.ACCEPTED,
                processor.process(quote(1, t, 10, 1), false).outcome());
        Quote other = new Quote(SYM, "other-src", 1, t, 10, 1, t);
        assertEquals(IngestionOutcome.ACCEPTED, processor.process(other, false).outcome());
        assertEquals(2L, processor.acceptedCount(SYM));
    }

    @Test
    void outOfOrderWithinToleranceIsAcceptedAndPlacedByEventTime() {
        long t = 1_000_000L;
        processor.process(quote(1, t, 10, 1), false);
        processor.process(quote(2, t + 1_000, 11, 1), false);
        // 晚到 3 秒，在 5 秒容忍窗口内：接受，但不覆盖更新时间更大的最新报价
        IngestionResult late = processor.process(quote(3, t - 3_000, 9, 5), false);
        assertEquals(IngestionOutcome.ACCEPTED, late.outcome());
        SnapshotView snap = processor.snapshot(SYM, false);
        assertEquals(t + 1_000, snap.lastQuote().eventTimeMillis());
        assertEquals(11, snap.lastQuote().price(), 0);
        assertEquals(3L, processor.acceptedCount(SYM));
    }

    @Test
    void beyondToleranceButWithinRollbackThresholdIsStale() {
        long t = 1_000_000L;
        processor.process(quote(1, t, 10, 1), false);
        IngestionResult r = processor.process(quote(2, t - 8_000, 10, 1), false);
        assertEquals(IngestionOutcome.REJECTED, r.outcome());
        assertEquals(RejectReason.STALE, r.reason());
        assertEquals(1L, processor.acceptedCount(SYM));
    }

    @Test
    void farBeyondRollbackThresholdIsClockRollback() {
        long t = 1_000_000L;
        processor.process(quote(1, t, 10, 1), false);
        IngestionResult r = processor.process(quote(2, t - 61_000, 10, 1), false);
        assertEquals(IngestionOutcome.REJECTED, r.outcome());
        assertEquals(RejectReason.CLOCK_ROLLBACK, r.reason());
        assertEquals(1L, processor.acceptedCount(SYM));
    }

    @Test
    void staleAndClockRollbackReasonsAreDistinctFromInvalidData() {
        long t = 1_000_000L;
        processor.process(quote(1, t, 10, 1), false);
        IngestionResult stale = processor.process(quote(2, t - 8_000, 10, 1), false);
        IngestionResult rollback = processor.process(quote(3, t - 70_000, 10, 1), false);
        IngestionResult invalidPrice = processor.process(
                new Quote(SYM, "src", 4, t + 100, -5, 1, t), false);
        IngestionResult invalidSize = processor.process(
                new Quote(SYM, "src", 5, t + 100, 10, 0, t), false);
        IngestionResult blankSymbol = processor.process(
                new Quote("  ", "src", 6, t + 100, 10, 1, t), false);
        assertEquals(RejectReason.STALE, stale.reason());
        assertEquals(RejectReason.CLOCK_ROLLBACK, rollback.reason());
        assertEquals(RejectReason.INVALID_DATA, invalidPrice.reason());
        assertEquals(RejectReason.INVALID_DATA, invalidSize.reason());
        assertEquals(RejectReason.INVALID_DATA, blankSymbol.reason());
    }

    @Test
    void futureEventTimeIsInvalidData() {
        long now = System.currentTimeMillis();
        Quote farFuture = new Quote(SYM, "src", 1, now + 3_600_000, 10, 1, now);
        assertEquals(RejectReason.INVALID_DATA, processor.process(farFuture, false).reason());
    }

    @Test
    void snapshotFreshnessFlagFlipsAfterTolerance() throws Exception {
        long base = System.currentTimeMillis();
        processor.process(new Quote(SYM, "src", 1, base, 10, 1, base), false);
        assertFalse(processor.snapshot(SYM, false).stale());
        Thread.sleep(3_200);
        SnapshotView staleSnap = processor.snapshot(SYM, false);
        assertTrue(staleSnap.stale());
        assertTrue(staleSnap.stalenessMillis() >= 3_000L);
        assertNotNull(staleSnap.trustNote());
    }

    @Test
    void recoveryPathIsIdempotentThroughSameProcessor() {
        // 模拟同一批历史被"恢复"两次：第二次应全部判重，接受计数不翻倍
        long t = 1_000_000L;
        Quote q1 = quote(1, t, 10, 1);
        Quote q2 = quote(2, t + 1_000, 11, 2);
        for (Quote q : new Quote[]{q1, q2}) {
            assertEquals(IngestionOutcome.ACCEPTED, processor.process(q, true).outcome());
        }
        for (Quote q : new Quote[]{q1, q2}) {
            assertEquals(IngestionOutcome.DUPLICATE, processor.process(q, true).outcome());
        }
        assertEquals(2L, processor.acceptedCount(SYM));
    }
}
