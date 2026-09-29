package com.github.xingjianluo.marketdatasnapshotservicejava.store;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SymbolView;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowStats;
import com.github.xingjianluo.marketdatasnapshotservicejava.window.WindowBucket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

/**
 * 内存行情存储。
 *
 * <p>并发模型：<b>单写线程</b>（{@code IngestService} 的处理线程）更新状态，
 * 任意多查询线程并发读取。每个品种持有一把 {@link StampedLock}：
 * <ul>
 *   <li>写路径加写锁，同时更新“最新价指针”和“窗口聚合”，因此查询不可能看到
 *       窗口已更新但最新价仍旧（或反之）的半更新状态；</li>
 *   <li>读路径加乐观读/读锁，在同一个临界区内拷贝出不可变的
 *       {@link SymbolView}/{@link WindowStats}，锁外再做序列化返回。</li>
 * </ul>
 * “最新价”定义为 eventTime 最大的行情（eventTime 相同时 eventId 字典序最大者），
 * 与到达顺序无关，乱序归位结果确定。
 */
public final class QuoteStore {

    private final long windowSizeMillis;
    private final int retainedWindows;
    private final ConcurrentHashMap<String, SymbolState> symbols = new ConcurrentHashMap<>();

    public QuoteStore(long windowSizeMillis, int retainedWindows) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("windowSizeMillis must be positive");
        }
        if (retainedWindows <= 0) {
            throw new IllegalArgumentException("retainedWindows must be positive");
        }
        this.windowSizeMillis = windowSizeMillis;
        this.retainedWindows = retainedWindows;
    }

    private SymbolState state(String symbol) {
        return symbols.computeIfAbsent(symbol,
                s -> new SymbolState(windowSizeMillis, retainedWindows));
    }

    /**
     * 应用一条已通过去重的行情。
     *
     * @return true 若它推进了最新价指针；false 若它是迟到（归位到窗口但不改变最新价）。
     */
    public boolean apply(Quote quote) {
        SymbolState st = state(quote.symbol());
        long stamp = st.lock.writeLock();
        try {
            boolean advanced;
            if (st.latest == null || later(quote, st.latest)) {
                st.latest = quote;
                advanced = true;
            } else {
                advanced = false;
            }
            long bucketStart = Math.floorDiv(quote.eventTime(), windowSizeMillis) * windowSizeMillis;
            st.buckets.computeIfAbsent(bucketStart,
                    b -> new WindowBucket(b, windowSizeMillis)).add(quote);
            prune(st);
            return advanced;
        } finally {
            st.lock.unlockWrite(stamp);
        }
    }

    private void prune(SymbolState st) {
        while (st.buckets.size() > retainedWindows) {
            st.buckets.pollFirstEntry();
        }
    }

    private static boolean later(Quote a, Quote b) {
        if (a.eventTime() != b.eventTime()) {
            return a.eventTime() > b.eventTime();
        }
        return a.eventId().compareTo(b.eventId()) > 0;
    }

    /**
     * 并发读：返回某品种一个一致时点快照；无数据返回 empty。
     * 快照在锁内完整拷贝，dataAge/stale 以 nowMillis 为准。
     */
    public Optional<SymbolView> view(String symbol, long nowMillis, long staleThresholdMillis) {
        SymbolState st = symbols.get(symbol);
        if (st == null) {
            return Optional.empty();
        }
        long stamp = st.lock.readLock();
        try {
            Quote latest = st.latest;
            if (latest == null) {
                return Optional.empty();
            }
            List<WindowStats> windows = new ArrayList<>(st.buckets.size());
            // 窗口按时间倒序，方便调用方先看最新窗口
            for (WindowBucket bucket : st.buckets.descendingMap().values()) {
                windows.add(bucket.snapshot());
            }
            long age = nowMillis - latest.eventTime();
            boolean stale = age > staleThresholdMillis || age < 0;
            return Optional.of(new SymbolView(symbol, latest, stale, age,
                    windowSizeMillis, windows));
        } finally {
            st.lock.unlockRead(stamp);
        }
    }

    /** 单写线程使用：乱序判定前读取当前最新 eventTime；无数据返回 MIN_VALUE。 */
    public long latestEventTime(String symbol) {
        SymbolState st = symbols.get(symbol);
        if (st == null) {
            return Long.MIN_VALUE;
        }
        Quote latest = st.latest;
        return latest == null ? Long.MIN_VALUE : latest.eventTime();
    }

    public Map<String, Long> latestEventTimes() {
        Map<String, Long> result = new HashMap<>();
        for (Map.Entry<String, SymbolState> e : symbols.entrySet()) {
            Quote latest = e.getValue().latest;
            if (latest != null) {
                result.put(e.getKey(), latest.eventTime());
            }
        }
        return result;
    }

    /** 检查点用：取某品种全部窗口聚合（时间正序）。 */
    public List<WindowStats> windowsOf(String symbol) {
        SymbolState st = symbols.get(symbol);
        if (st == null) {
            return List.of();
        }
        long stamp = st.lock.readLock();
        try {
            List<WindowStats> result = new ArrayList<>(st.buckets.size());
            for (WindowBucket bucket : st.buckets.values()) {
                result.add(bucket.snapshot());
            }
            return result;
        } finally {
            st.lock.unlockRead(stamp);
        }
    }

    /** 检查点用：某品种最新价。 */
    public Quote latestOf(String symbol) {
        SymbolState st = symbols.get(symbol);
        return st == null ? null : st.latest;
    }

    public java.util.Set<String> symbols() {
        return symbols.keySet();
    }

    /**
     * 恢复装载（必须在处理线程启动、任何写入之前完成）。
     * 装载的是检查点固化状态；WAL 尾部随后经 {@link #apply} 补入，
     * 重复条目由 DedupIndex 拦截，因此幂等。
     */
    public void restore(Map<String, Quote> latestBySymbol,
                        Map<String, List<WindowStats>> windowsBySymbol) {
        if (latestBySymbol != null) {
            for (Map.Entry<String, Quote> e : latestBySymbol.entrySet()) {
                SymbolState st = state(e.getKey());
                st.latest = e.getValue();
            }
        }
        if (windowsBySymbol != null) {
            for (Map.Entry<String, List<WindowStats>> e : windowsBySymbol.entrySet()) {
                SymbolState st = state(e.getKey());
                for (WindowStats ws : e.getValue()) {
                    WindowBucket bucket = new WindowBucket(ws.windowStart(), windowSizeMillis);
                    // open/close 价格在检查点中不携带完整 Quote，恢复后桶只保留统计数值；
                    // 后续 WAL 重放若覆盖该桶会重新补全 open/close。
                    bucket.load(ws.count(), ws.high(), ws.low(), ws.totalVolume(),
                            ws.openPrice(), ws.closePrice());
                    st.buckets.put(ws.windowStart(), bucket);
                }
                prune(st);
            }
        }
    }

    private static final class SymbolState {
        private final long windowSizeMillis;
        private final int retainedWindows;
        private final StampedLock lock = new StampedLock();
        private volatile Quote latest;
        private final TreeMap<Long, WindowBucket> buckets = new TreeMap<>();

        private SymbolState(long windowSizeMillis, int retainedWindows) {
            this.windowSizeMillis = windowSizeMillis;
            this.retainedWindows = retainedWindows;
        }
    }
}
