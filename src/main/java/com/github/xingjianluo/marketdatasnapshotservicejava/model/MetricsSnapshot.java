package com.github.xingjianluo.marketdatasnapshotservicejava.model;

import java.util.Map;

/**
 * 运行状态与背压可观察指标。
 */
public record MetricsSnapshot(long received, long accepted, long outOfOrderAccepted,
                              long duplicates, long invalidData, long expired,
                              long clockRollback, long backpressureRejected,
                              long persistenceFailures, int queueDepth, int queueCapacity,
                              long oldestQueuedAgeMillis, long checkpointCount,
                              long lastCheckpointTime, boolean degraded, String degradedReason,
                              Map<String, Long> latestEventTimeBySymbol,
                              Map<String, Long> dataAgeMillisBySymbol) {
}
