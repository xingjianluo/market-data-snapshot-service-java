package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.MutableClock;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.TestServiceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 数据新鲜度/可信度：查询结果用 stale 与 dataAgeMillis 显式说明当前数据是否可信，
 * 与“消息是否被计入”是两件事——源停更后历史快照仍可查，但被标记为 stale。
 */
class FreshnessTest {

    @TempDir
    Path stateDir;

    @Test
    void staleFlagTransitionsAsClockAdvances() {
        MutableClock clock = new MutableClock(1_700_000_000_000L);
        ServiceProperties props = TestServiceFactory.props(stateDir);
        props.setStaleThresholdMillis(5_000);
        props.setCheckpointIntervalMessages(1_000_000);
        props.setCheckpointIntervalMillis(1_000_000);
        IngestService svc = TestServiceFactory.started(props, clock);
        try {
            svc.submit(new QuoteRequest("f1", "BTCUSDT", 1_700_000_000_000L, 100L, 1L));

            SymbolView fresh = svc.query("BTCUSDT").orElseThrow();
            assertFalse(fresh.stale());
            assertEquals(0L, fresh.dataAgeMillis());

            clock.advance(4_999);
            assertFalse(svc.query("BTCUSDT").orElseThrow().stale());

            clock.advance(2); // 5001ms
            SymbolView stale = svc.query("BTCUSDT").orElseThrow();
            assertTrue(stale.stale(), "data older than threshold must be flagged untrusted");
            assertEquals(5_001L, stale.dataAgeMillis());

            // 指标里也给出每品种数据年龄
            assertEquals(5_001L, svc.metrics().dataAgeMillisBySymbol().get("BTCUSDT"));
        } finally {
            svc.shutdown();
        }
    }
}
