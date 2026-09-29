package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowStats;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.MutableClock;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.TestServiceFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 本地状态恢复：
 * 1) 无检查点、仅有 WAL：重启后从 WAL 重放恢复；
 * 2) 检查点 + WAL：两者合并，结果与连续运行一致；
 * 3) 重复恢复（再启动一次）历史数据不重复累计；
 * 4) 重启后重发旧 eventId 仍判 DUPLICATE。
 */
class RestartRecoveryTest {

    private static final long T0 = 1_700_000_000_000L;

    @TempDir
    Path stateDir;

    private static QuoteRequest q(String id, long t) {
        return new QuoteRequest(id, "BTCUSDT", t, 100L, 2L);
    }

    private IngestService boot(MutableClock clock, boolean autoCheckpoint) {
        ServiceProperties props = TestServiceFactory.props(stateDir);
        if (!autoCheckpoint) {
            props.setCheckpointIntervalMessages(1_000_000);
            props.setCheckpointIntervalMillis(1_000_000);
        }
        return TestServiceFactory.started(props, clock);
    }

    @Test
    void replaysWalWhenNoCheckpointExists() {
        MutableClock clock = new MutableClock(T0);

        IngestService first = boot(clock, false);
        first.submit(q("w1", T0 + 100));
        first.submit(q("w2", T0 + 1_200));
        first.shutdownWithoutFinalCheckpointForTest(); // 保留 WAL 不做最终检查点

        clock.set(T0 + 2_000);
        IngestService second = boot(clock, false);
        SymbolView view = second.query("BTCUSDT").orElseThrow();
        assertEquals("w2", view.latest().eventId());
        long totalCount = view.windows().stream().mapToLong(WindowStats::count).sum();
        assertEquals(2, totalCount);
        second.shutdown();
    }

    @Test
    void checkpointPlusWalAndDoubleRecoveryDoesNotDoubleCount() {
        MutableClock clock = new MutableClock(T0);
        IngestService first = boot(clock, true); // 自动检查点：每 1000 条；改用手动
        first.submit(q("c1", T0 + 100));
        first.submit(q("c2", T0 + 200));
        first.checkpointNow();                 // c1/c2 进入检查点，WAL 被轮转清空
        first.submit(q("c3", T0 + 1_100));     // 落在检查点之后、仅存在于 WAL
        first.shutdownWithoutFinalCheckpointForTest();

        clock.set(T0 + 5_000);
        IngestService second = boot(clock, false);
        SymbolView v1 = second.query("BTCUSDT").orElseThrow();
        assertEquals("c3", v1.latest().eventId());
        long countAfterFirstRecover = v1.windows().stream().mapToLong(WindowStats::count).sum();
        assertEquals(3, countAfterFirstRecover);

        // 重启后旧 eventId 仍被识别为重复，不能重新计入
        var dup = second.submit(q("c2", T0 + 200));
        assertEquals(RejectReason.DUPLICATE, dup.reason());
        second.shutdownWithoutFinalCheckpointForTest();

        // 第二次恢复（重复恢复）：计数必须保持 3，不翻倍
        clock.set(T0 + 6_000);
        IngestService third = boot(clock, false);
        SymbolView v2 = third.query("BTCUSDT").orElseThrow();
        long countAfterSecondRecover = v2.windows().stream().mapToLong(WindowStats::count).sum();
        assertEquals(3, countAfterSecondRecover);
        assertEquals("c3", v2.latest().eventId());
        third.shutdown();
    }

    @Test
    void freshStateDirHasNoData() {
        MutableClock clock = new MutableClock(T0);
        IngestService svc = boot(clock, false);
        assertTrue(svc.query("BTCUSDT").isEmpty());
        assertNotNull(svc.metrics());
        svc.shutdown();
    }
}
