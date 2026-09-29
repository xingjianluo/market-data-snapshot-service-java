package com.github.xingjianluo.marketdatasnapshotservicejava.tests;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.AppendEventLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppendEventLogTest {

    private Quote q(long seq, long t) {
        return new Quote("S", "src", seq, t, 100 + seq, seq, t);
    }

    @Test
    void appendThenReplayPreservesRecords(@TempDir Path dir) {
        AppendEventLog log = new AppendEventLog(dir.toString(), "quotes", 1024 * 1024, false);
        log.append(q(1, 1_000));
        log.append(q(2, 2_000));
        log.append(q(3, 3_000));
        log.close();

        AppendEventLog reopened = new AppendEventLog(dir.toString(), "quotes", 1024 * 1024, false);
        List<Quote> replayed = reopened.replay();
        assertEquals(3, replayed.size());
        assertEquals(1L, replayed.get(0).seqNum());
        assertEquals(3L, replayed.get(2).seqNum());
        assertEquals(101.0, replayed.get(0).price(), 0);
    }

    @Test
    void replayIsStableAcrossRepeatedReads(@TempDir Path dir) {
        AppendEventLog log = new AppendEventLog(dir.toString(), "quotes", 1024 * 1024, false);
        for (long i = 1; i <= 10; i++) {
            log.append(q(i, i * 10));
        }
        List<Quote> first = log.replay();
        List<Quote> second = log.replay();
        assertEquals(first, second);
    }

    @Test
    void rotationKeepsOldestSegmentAndReplaysOldToNew(@TempDir Path dir) {
        // 每条约 110~130 字节；maxFileBytes=160 会频繁滚动，仅保留 .1
        AppendEventLog log = new AppendEventLog(dir.toString(), "quotes", 160, false);
        for (long i = 1; i <= 8; i++) {
            log.append(q(i, i * 100));
        }
        log.close();

        assertTrue(Files.exists(dir.resolve("quotes.log")));
        assertTrue(Files.exists(dir.resolve("quotes.log.1")));

        List<Quote> all = new AppendEventLog(dir.toString(), "quotes", 160, false).replay();
        // 两段：滚动段（较旧）在前，活动段在后；段内顺序保留。最早的若干条随再次滚动被覆盖丢弃，
        // 这是单段滚动的容量语义，恢复只承诺"保留段内的接受历史"。
        assertTrue(all.size() >= 2);
        for (int i = 1; i < all.size(); i++) {
            assertTrue(all.get(i - 1).eventTimeMillis() <= all.get(i).eventTimeMillis(),
                    "replay order must be old-to-new within/between segments");
        }
    }

    @Test
    void corruptLinesAreSkippedWithoutKillingReplay(@TempDir Path dir) throws Exception {
        AppendEventLog log = new AppendEventLog(dir.toString(), "quotes", 1024 * 1024, false);
        log.append(q(1, 1_000));
        log.close();
        Path active = dir.resolve("quotes.log");
        Files.writeString(active,
                Files.readString(active) + "{ this is not json\n\n");

        List<Quote> replayed = new AppendEventLog(dir.toString(), "quotes", 1024 * 1024, false)
                .replay();
        assertEquals(1, replayed.size());
        assertEquals(1L, replayed.get(0).seqNum());
    }
}
