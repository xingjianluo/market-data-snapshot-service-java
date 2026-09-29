package com.github.xingjianluo.marketdatasnapshotservicejava.store;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowAggregate;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 单个品种的全部可变状态。<b>所有字段只允许在持有本对象监视器锁时读写</b>。
 * 单消费者线程在同一把锁内完成"判重 → 乱序/回拨判定 → 更新最新价 → 更新窗口"的复合操作，
 * 因此并发查询者要么看到更新前、要么看到更新后的完整状态，不会看到半更新。
 *
 * <p>聚合窗口定义：固定长度 {@code windowLengthMillis} 的尾随窗口，区间为
 * {@code (maxEventTime - windowLength, maxEventTime]}（左开右闭）。
 * 窗口内按 dedupKey 保存报价，因此：
 * <ul>
 *   <li>重复消息（在线重复或重启后重放）不会再次计入——聚合幂等；</li>
 *   <li>窗口随最大事件时间只向前推进，越界报价被确定性剔除，结果与到达顺序无关；</li>
 *   <li>open/close 按窗口内事件时间最早/最晚选取，乱序归位后结果稳定。</li>
 * </ul>
 */
public final class SymbolState {

    private final String symbol;
    private final DedupCache dedupCache;
    private final long windowLengthMillis;

    private long maxEventTimeMillis = Long.MIN_VALUE;
    private Quote latest;
    private long acceptedCount;

    /** dedupKey -> 仍在尾随窗口内的报价（遍历顺序即插入顺序，仅用于稳定的调试输出）。 */
    private final Map<String, Quote> windowQuotes = new LinkedHashMap<>();

    public SymbolState(String symbol, int dedupCacheCapacity, long windowLengthMillis) {
        if (windowLengthMillis <= 0) {
            throw new IllegalArgumentException("windowLengthMillis must be positive");
        }
        this.symbol = symbol;
        this.dedupCache = new DedupCache(dedupCacheCapacity);
        this.windowLengthMillis = windowLengthMillis;
    }

    public String getSymbol() {
        return symbol;
    }

    public long getMaxEventTimeMillis() {
        return maxEventTimeMillis;
    }

    public Quote getLatest() {
        return latest;
    }

    public long getAcceptedCount() {
        return acceptedCount;
    }

    public int windowSize() {
        return windowQuotes.size();
    }

    /** 首次见到该 dedupKey 返回 true；重复返回 false。 */
    public boolean markSeen(String dedupKey) {
        return dedupCache.addIfAbsent(dedupKey);
    }

    /**
     * 接受一条报价：更新最大事件时间与最新报价，并维护尾随窗口。
     * 调用前必须已通过判重与乱序/回拨判定；调用必须在锁内。
     */
    public void accept(Quote quote) {
        if (quote.eventTimeMillis() > maxEventTimeMillis) {
            maxEventTimeMillis = quote.eventTimeMillis();
        }
        // 最新报价按事件时间决定：被归位的乱序旧报价不会覆盖更新的快照
        if (latest == null || quote.eventTimeMillis() >= latest.eventTimeMillis()) {
            latest = quote;
        }
        windowQuotes.put(quote.dedupKey(), quote);
        evictOutOfWindow();
        acceptedCount++;
    }

    /** 是否已经接受过至少一条报价（注册表项可能先于数据存在）。 */
    public boolean hasData() {
        return latest != null;
    }

    /**
     * 当前尾随窗口聚合（锁内调用）；窗口内无报价时返回 null。
     */
    public WindowAggregate currentAggregate() {
        evictOutOfWindow();
        if (windowQuotes.isEmpty()) {
            return null;
        }
        Quote open = null;
        Quote close = null;
        double high = Double.NEGATIVE_INFINITY;
        double low = Double.POSITIVE_INFINITY;
        double notional = 0d;
        double volume = 0d;
        for (Quote q : windowQuotes.values()) {
            if (open == null || q.eventTimeMillis() < open.eventTimeMillis()) {
                open = q;
            }
            if (close == null || q.eventTimeMillis() >= close.eventTimeMillis()) {
                close = q;
            }
            high = Math.max(high, q.price());
            low = Math.min(low, q.price());
            volume += q.size();
            notional += q.price() * q.size();
        }
        double vwap = volume > 0 ? notional / volume : 0d;
        long windowEnd = maxEventTimeMillis;
        long windowStart = windowEnd - windowLengthMillis;
        return new WindowAggregate(windowStart, windowEnd, windowQuotes.size(),
                open.price(), high, low, close.price(), volume, vwap);
    }

    /**
     * 剔除严格早于 {@code maxEventTime - windowLength} 的报价。
     * 剔除只依赖当前最大事件时间与报价自身时间，因此任何重放顺序下结果一致。
     */
    private void evictOutOfWindow() {
        long cutoff = maxEventTimeMillis - windowLengthMillis;
        Iterator<Map.Entry<String, Quote>> it = windowQuotes.entrySet().iterator();
        while (it.hasNext()) {
            Quote q = it.next().getValue();
            if (q.eventTimeMillis() <= cutoff) {
                it.remove();
            }
        }
    }
}
