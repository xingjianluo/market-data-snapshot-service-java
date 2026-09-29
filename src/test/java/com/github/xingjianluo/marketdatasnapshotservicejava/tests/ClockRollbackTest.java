package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.QuoteRequest;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.MutableClock;
import com.github.xingjianluo.marketdatasnapshotservicejava.tests.support.TestServiceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 时钟回拨/漂移与业务“过期”、小幅“乱序”之间的区分：
 * - 相对服务端接收时间超出 rollback 年龄 -> CLOCK_ROLLBACK（即使它在序列里很“新”）；
 * - 同一条消息先因时钟回拨被拒，时钟恢复后重试，eventId 不被占用，可正常计入。
 */
class ClockRollbackTest {

    @TempDir
    Path stateDir;
    private IngestService svc;

    @AfterEach
    void tearDown() {
        if (svc != null) {
            svc.shutdown();
        }
    }

    private QuoteRequest q(String id, long eventTime) {
        return new QuoteRequest(id, "ETHUSDT", eventTime, 100L, 1L);
    }

    @Test
    void farPastEventTimeIsClockRollbackRatherThanExpired() {
        MutableClock clock = new MutableClock(1_700_000_000_000L);
        ServiceProperties props = TestServiceFactory.props(stateDir);
        svc = TestServiceFactory.started(props, clock);

        assertEquals(RejectReason.ACCEPTED, svc.submit(q("normal", 1_700_000_000_000L - 1_000L)).reason());

        // eventTime 比 server 接收时间早超过 24h：时钟回拨，而非业务过期
        IngestResult ancient = svc.submit(q("ancient", 1_700_000_000_000L - 25 * 60 * 60_000L));
        assertEquals(RejectReason.CLOCK_ROLLBACK, ancient.reason());
        assertEquals(1L, svc.metrics().clockRollback());
        assertEquals(0L, svc.metrics().expired());
    }

    @Test
    void farFutureEventTimeIsClockDriftRollback() {
        MutableClock clock = new MutableClock(1_700_000_000_000L);
        ServiceProperties props = TestServiceFactory.props(stateDir);
        svc = TestServiceFactory.started(props, clock);

        IngestResult future = svc.submit(q("future", 1_700_000_000_000L + 25 * 60 * 60_000L));
        assertEquals(RejectReason.CLOCK_ROLLBACK, future.reason());
    }

    @Test
    void rejectedClockRollbackDoesNotConsumeEventIdAndRetrySucceeds() {
        MutableClock clock = new MutableClock(1_700_000_000_000L);
        ServiceProperties props = TestServiceFactory.props(stateDir);
        svc = TestServiceFactory.started(props, clock);

        QuoteRequest bad = q("retry-me", 1_700_000_000_000L - 25 * 60 * 60_000L);
        assertEquals(RejectReason.CLOCK_ROLLBACK, svc.submit(bad).reason());

        QuoteRequest fixed = q("retry-me", 1_700_000_000_000L + 100L); // 时钟恢复后同一 eventId 重发
        assertEquals(RejectReason.ACCEPTED, svc.submit(fixed).reason());
        assertTrue(svc.query("ETHUSDT").isPresent());
        assertEquals("retry-me", svc.query("ETHUSDT").orElseThrow().latest().eventId());
    }
}
