package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowStats;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.MutableClock;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.TestServiceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 乱序归位 / 重复 / 过期判定与窗口聚合幂等性。
 */
class OrderingDedupTest {

    @TempDir
    Path stateDir;
    private IngestService svc;

    private IngestService start(MutableClock clock) {
        ServiceProperties props = TestServiceFactory.props(stateDir);
        props.setCheckpointIntervalMessages(1_000_000); // 本测试禁用自动检查点
        props.setCheckpointIntervalMillis(1_000_000);
        svc = TestServiceFactory.started(props, clock);
        return svc;
    }

    @AfterEach
    void tearDown() {
        if (svc != null) {
            svc.shutdown();
        }
    }

    private static QuoteRequest q(String id, long t, long price, long size) {
        return new QuoteRequest(id, "BTCUSDT", t, price, size);
    }

    @Test
    void outOfOrderIsPlacedIntoItsEventTimeWindowAndDoesNotChangeLatest() {
        MutableClock clock = new MutableClock(100_000);
        svc = start(clock);

        // 窗口 100000~100999：先发 100200
        IngestResult r1 = svc.submit(q("e1", 100_200, 100, 5));
        assertEquals(RejectReason.ACCEPTED, r1.reason());

        // 乱序：100100 在同窗口更早位置（lag 100ms < 5000ms）
        IngestResult r2 = svc.submit(q("e2", 100_100, 90, 3));
        assertEquals(RejectReason.OUT_OF_ORDER, r2.reason());
        assertTrue(r2.accepted());

        // 乱序归位不改变最新价指针：最新仍是 e1（eventTime 更大）
        SymbolView view = svc.query("BTCUSDT").orElseThrow();
        assertEquals("e1", view.latest().eventId());
        assertEquals(100, view.latest().price());

        // 窗口聚合按 eventTime 排序确定 open/close，与到达顺序无关：
        // open=e2(90, 更早)，close=e1(100)，high=100，low=90，量=8
        WindowStats w = view.windows().get(0);
        assertEquals(100_000, w.windowStart());
        assertEquals(2, w.count());
        assertEquals(90L, w.openPrice());
        assertEquals(100L, w.closePrice());
        assertEquals(100L, w.high());
        assertEquals(90L, w.low());
        assertEquals(8L, w.totalVolume());
    }

    @Test
    void duplicateEventIsRejectedAndNotRecounted() {
        MutableClock clock = new MutableClock(100_000);
        svc = start(clock);
        QuoteRequest request = q("dup-1", 100_000, 100, 7);

        assertEquals(RejectReason.ACCEPTED, svc.submit(request).reason());
        IngestResult again = svc.submit(request);
        assertEquals(RejectReason.DUPLICATE, again.reason());
        assertFalse(again.accepted());

        WindowStats w = svc.query("BTCUSDT").orElseThrow().windows().get(0);
        assertEquals(1, w.count());
        assertEquals(7L, w.totalVolume());
        assertEquals(1L, svc.metrics().duplicates());
    }

    @Test
    void tooOldMessageIsExpiredWithDistinctReason() {
        MutableClock clock = new MutableClock(200_000);
        svc = start(clock);
        assertEquals(RejectReason.ACCEPTED, svc.submit(q("new", 200_000, 100, 1)).reason());
        // 落后 60s，超过 TTL
        IngestResult stale = svc.submit(q("old", 140_000, 100, 1));
        assertEquals(RejectReason.EXPIRED, stale.reason());
        assertEquals(1L, svc.metrics().expired());
    }

    @Test
    void invalidDataIsRejectedWithDistinctReason() {
        MutableClock clock = new MutableClock(100_000);
        svc = start(clock);
        assertEquals(RejectReason.INVALID_DATA,
                svc.submit(new QuoteRequest(null, "BTC", 1L, 1L, 1L)).reason());
        assertEquals(RejectReason.INVALID_DATA,
                svc.submit(new QuoteRequest("x", "BTC", null, 1L, 1L)).reason());
        assertEquals(RejectReason.INVALID_DATA,
                svc.submit(new QuoteRequest("x", "BTC", 1L, 0L, 1L)).reason());
        assertEquals(3L, svc.metrics().invalidData());
        assertTrue(svc.query("BTC").isEmpty());
    }

    @Test
    void unknownSymbolQueryIsEmpty() {
        MutableClock clock = new MutableClock(100_000);
        svc = start(clock);
        Optional<SymbolView> view = svc.query("NOPE");
        assertTrue(view.isEmpty());
    }
}
