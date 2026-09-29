package com.github.xingjianluo.marketdatasnapshotservicejava.tests.support;

import com.github.xingjianluo.marketdatasnapshotservicejava.clock.Clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试可控时钟，可前拨/回拨以复现乱序、时钟回拨与 staleness。
 */
public class MutableClock implements Clock {

    private final AtomicLong now;

    public MutableClock(long start) {
        this.now = new AtomicLong(start);
    }

    @Override
    public long nowMillis() {
        return now.get();
    }

    public void set(long millis) {
        now.set(millis);
    }

    public void advance(long deltaMillis) {
        now.addAndGet(deltaMillis);
    }
}
