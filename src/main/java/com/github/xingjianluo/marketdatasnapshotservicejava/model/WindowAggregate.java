package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 一个时间窗口内的幂等聚合（OHLC + 成交量 + VWAP）。
 * 聚合按消息去重键幂等：重复/重放不会改变结果。
 */
public record WindowAggregate(
        long windowStartMillis,
        long windowEndMillis,
        int tickCount,
        double open,
        double high,
        double low,
        double close,
        double volume,
        double vwap) {
}
