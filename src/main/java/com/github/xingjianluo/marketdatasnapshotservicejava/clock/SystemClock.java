package com.github.xingjianluo.marketdatasnapshotservicejava.clock;

public class SystemClock implements Clock {
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
