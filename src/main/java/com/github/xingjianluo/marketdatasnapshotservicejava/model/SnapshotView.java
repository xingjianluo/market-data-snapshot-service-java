package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 单个品种某一时点的一致快照（不可变）。
 * lastUpdateEventTimeMillis 为本品种已接受数据中的最大事件时间。
 * stale 表示按事件时间判断数据已超过新鲜度容忍；degraded 表示服务处于降级态（如背压拒绝中）。
 */
public record SnapshotView(
        String symbol,
        Quote lastQuote,
        long lastUpdateEventTimeMillis,
        WindowAggregate window,
        boolean stale,
        boolean degraded,
        long stalenessMillis,
        String trustNote) {
}
