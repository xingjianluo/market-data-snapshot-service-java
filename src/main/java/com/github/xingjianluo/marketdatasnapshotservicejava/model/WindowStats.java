package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 单个滚动时间窗口内的聚合统计（不可变快照）。
 */
public record WindowStats(long windowStart, long windowEnd, long count,
                          Long openPrice, long high, long low, Long closePrice,
                          long totalVolume) {
}
