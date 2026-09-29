package com.github.xingjianluo.marketdatasnapshotservicejava.model;

import java.util.Map;

/**
 * 运行状态观测快照。
 * queueCapacity/queueSize 反映处理通道积压程度；各拒绝原因分别计数。
 */
public record MetricsSnapshot(
        long received,
        long accepted,
        long duplicate,
        long rejected,
        long rejectedInvalid,
        long rejectedStale,
        long rejectedClockRollback,
        long rejectedBackpressure,
        int queueCapacity,
        int queueSize,
        long recovered,
        boolean degraded,
        long uptimeMillis,
        Map<String, Long> acceptedBySymbol) {
}
