package com.github.xingjianluo.marketdatasnapshotservicejava.clock;

/**
 * 可替换时钟，便于测试乱序/回拨/恢复语义。
 */
public interface Clock {
    long nowMillis();
}
