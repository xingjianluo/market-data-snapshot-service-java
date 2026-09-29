package com.github.xingjianluo.marketdatasnapshotservicejava.window;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowStats;

/**
 * 单个滚动时间窗口桶（左闭右开 [start, start+size)）。
 *
 * <p>聚合规则与消息到达顺序无关，保证乱序归位后结果确定：
 * <ul>
 *   <li>open  = 窗口内 eventTime 最小（相同则 eventId 字典序最小）的行情价格</li>
 *   <li>close = 窗口内 eventTime 最大（相同则 eventId 字典序最大）的行情价格</li>
 *   <li>high/low = 价格极值；count/totalVolume = 条数与成交量之和</li>
 * </ul>
 * 幂等性：同一 eventId 不会被调用两次（由去重索引保证），因此聚会计数天然不重复。
 * 本类非线程安全，由 QuoteStore 的每品种锁保护。
 */
public final class WindowBucket {

    private final long start;
    private final long sizeMillis;

    private long count;
    private Quote openQuote;
    private Quote closeQuote;
    private long high;
    private long low;
    private long totalVolume;
    private Long restoredOpen;
    private Long restoredClose;

    public WindowBucket(long start, long sizeMillis) {
        this.start = start;
        this.sizeMillis = sizeMillis;
    }

    public long start() {
        return start;
    }

    /** 把一条已去重的行情计入窗口；乱序到达时 open/close 会被重新修正。 */
    public void add(Quote quote) {
        count++;
        totalVolume += quote.size();
        if (count == 1) {
            high = low = quote.price();
            openQuote = closeQuote = quote;
            return;
        }
        high = Math.max(high, quote.price());
        low = Math.min(low, quote.price());
        if (earlier(quote, openQuote)) {
            openQuote = quote;
        }
        if (later(quote, closeQuote)) {
            closeQuote = quote;
        }
    }

    /** 从检查点恢复已聚合状态时使用：直接装载统计，不再逐条重放。 */
    public void load(long count, long high, long low, long totalVolume,
                     Long openPrice, Long closePrice) {
        this.count = count;
        this.high = high;
        this.low = low;
        this.totalVolume = totalVolume;
        this.restoredOpen = openPrice;
        this.restoredClose = closePrice;
    }

    private static boolean earlier(Quote a, Quote b) {
        if (a.eventTime() != b.eventTime()) {
            return a.eventTime() < b.eventTime();
        }
        return a.eventId().compareTo(b.eventId()) < 0;
    }

    private static boolean later(Quote a, Quote b) {
        if (a.eventTime() != b.eventTime()) {
            return a.eventTime() > b.eventTime();
        }
        return a.eventId().compareTo(b.eventId()) > 0;
    }

    public WindowStats snapshot() {
        Long open = openQuote == null ? restoredOpen : openQuote.price();
        Long close = closeQuote == null ? restoredClose : closeQuote.price();
        return new WindowStats(start, start + sizeMillis, count,
                open, high, low, close, totalVolume);
    }

    public long count() {
        return count;
    }

    public long high() {
        return high;
    }

    public long low() {
        return low;
    }

    public long totalVolume() {
        return totalVolume;
    }

    public Quote openQuote() {
        return openQuote;
    }

    public Quote closeQuote() {
        return closeQuote;
    }
}
