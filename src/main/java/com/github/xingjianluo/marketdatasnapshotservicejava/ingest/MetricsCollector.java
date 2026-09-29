package com.github.xingjianluo.marketdatasnapshotservicejava.ingest;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 线程安全运行指标。所有计数只增不减，降级标志可翻转。
 * 拒绝原因各自独立计数，DUPLICATE/EXPIRED/CLOCK_ROLLBACK/BACKPRESSURE/
 * INVALID_DATA/PERSISTENCE_FAILURE 在指标与日志中都可区分。
 */
public final class MetricsCollector {

    private final AtomicLong received = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong outOfOrderAccepted = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong invalidData = new AtomicLong();
    private final AtomicLong expired = new AtomicLong();
    private final AtomicLong clockRollback = new AtomicLong();
    private final AtomicLong backpressureRejected = new AtomicLong();
    private final AtomicLong persistenceFailures = new AtomicLong();
    private final AtomicLong checkpointCount = new AtomicLong();
    private final AtomicLong lastCheckpointTime = new AtomicLong();

    private final AtomicReference<DegradedState> degraded =
            new AtomicReference<>(DegradedState.ok());

    private record DegradedState(boolean degraded, String reason) {
        static DegradedState ok() {
            return new DegradedState(false, null);
        }
    }

    public void incrementReceived() {
        received.incrementAndGet();
    }

    public void recordOutcome(RejectReason reason) {
        switch (reason) {
            case ACCEPTED -> accepted.incrementAndGet();
            case OUT_OF_ORDER -> {
                accepted.incrementAndGet();
                outOfOrderAccepted.incrementAndGet();
            }
            case DUPLICATE -> duplicates.incrementAndGet();
            case INVALID_DATA -> invalidData.incrementAndGet();
            case EXPIRED -> expired.incrementAndGet();
            case CLOCK_ROLLBACK -> clockRollback.incrementAndGet();
            case BACKPRESSURE -> backpressureRejected.incrementAndGet();
            case PERSISTENCE_FAILURE -> persistenceFailures.incrementAndGet();
            case SERVICE_CLOSED -> { /* 不计入数据类拒绝 */ }
        }
    }

    public long incrementCheckpointCount(long nowMillis) {
        lastCheckpointTime.set(nowMillis);
        return checkpointCount.incrementAndGet();
    }

    public void setDegraded(boolean degraded, String reason) {
        this.degraded.set(degraded ? new DegradedState(true, reason) : DegradedState.ok());
    }

    public MetricsSnapshot snapshot(int queueDepth, int queueCapacity, long oldestQueuedAge,
                                    Map<String, Long> latestEventTimeBySymbol,
                                    Map<String, Long> dataAgeBySymbol) {
        DegradedState d = degraded.get();
        return new MetricsSnapshot(
                received.get(), accepted.get(), outOfOrderAccepted.get(),
                duplicates.get(), invalidData.get(), expired.get(),
                clockRollback.get(), backpressureRejected.get(),
                persistenceFailures.get(), queueDepth, queueCapacity,
                oldestQueuedAge, checkpointCount.get(), lastCheckpointTime.get(),
                d.degraded(), d.reason(),
                Map.copyOf(latestEventTimeBySymbol), Map.copyOf(dataAgeBySymbol));
    }

    public long received() {
        return received.get();
    }

    public long acceptedCount() {
        return accepted.get();
    }
}
