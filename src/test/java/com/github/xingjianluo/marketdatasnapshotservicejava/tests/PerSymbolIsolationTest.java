package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.OrderedProcessor;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.ServiceMetrics;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.InMemoryStateStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 品种间隔离：乱序/回拨/去重/窗口都按品种独立维护。
 * A 品种发生时钟回拨，不影响 B 品种的正常时间线；同一 (source,seqNum) 在不同品种各自独立。
 */
class PerSymbolIsolationTest {

    @Test
    void rollbackAndOrderingAreTrackedPerSymbol() {
        ServiceProperties props = new ServiceProperties();
        props.setDisorderToleranceMillis(5_000L);
        props.setClockRollbackThresholdMillis(60_000L);
        InMemoryStateStore store = new InMemoryStateStore(1000, 60_000L);
        OrderedProcessor processor = new OrderedProcessor(store, props, new ServiceMetrics());

        long t = 5_000_000_000L;
        // A 品种推进到 t+100000
        processor.process(new Quote("AAA", "src", 1, t, 10, 1, t), false);
        processor.process(new Quote("AAA", "src", 2, t + 100_000, 10, 1, t), false);
        // A 品种再收到 t-1000 -> 相对 A 的 max 落后 101 秒 -> CLOCK_ROLLBACK
        assertEquals(RejectReason.CLOCK_ROLLBACK,
                processor.process(new Quote("AAA", "src", 3, t - 1_000, 10, 1, t), false).reason());

        // B 品种的第一条消息即使是较小的绝对时间，也只是首条，不能受 A 的时间线影响
        assertEquals(IngestionOutcome.ACCEPTED,
                processor.process(new Quote("BBB", "src", 1, 4_000_000_000L, 20, 2, 4_000_000_000L), false).outcome());
        // B 品种继续自己的时间线
        assertEquals(IngestionOutcome.ACCEPTED,
                processor.process(new Quote("BBB", "src", 2, 4_000_001_000L, 21, 2, 4_000_001_000L), false).outcome());
        // B 品种回拨独立判定
        assertEquals(RejectReason.CLOCK_ROLLBACK,
                processor.process(new Quote("BBB", "src", 3, 4_000_001_000L - 61_000, 21, 2, 4_000_001_000L), false).reason());

        // 同 (source,seqNum) 跨品种独立：AAA 已有 src#1，BBB 的 src#1 是首条（上面已接受）。
        // BBB 继续接收新 seq
        assertEquals(IngestionOutcome.ACCEPTED,
                processor.process(new Quote("BBB", "src", 4, 4_000_002_000L, 22, 2, 4_000_002_000L), false).outcome());
        // 重发 BBB 已见过的 src#1（即使内容/时间不同）仍判重
        assertEquals(IngestionOutcome.DUPLICATE,
                processor.process(new Quote("BBB", "src", 1, 4_000_002_000L, 22, 2, 4_000_002_000L), false).outcome());

        assertEquals(2L, processor.acceptedCount("AAA"));
        assertEquals(3L, processor.acceptedCount("BBB"));
    }
}
