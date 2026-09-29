package com.github.xingjianluo.marketdatasnapshotservicejava.engine;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionOutcome;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.MetricsSnapshot;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * 运行指标（全部线程安全，读取为近似一致的瞬时值）：
 * 总量、各结论分类计数、各拒绝原因分别计数、恢复计数，
 * 以及队列容量/当前积压（由服务层提供）与降级标志。
 */
public final class ServiceMetrics {

    private final LongAdder received = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder duplicate = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder rejectedInvalid = new LongAdder();
    private final LongAdder rejectedStale = new LongAdder();
    private final LongAdder rejectedClockRollback = new LongAdder();
    private final LongAdder rejectedBackpressure = new LongAdder();
    private final LongAdder recovered = new LongAdder();

    private final ConcurrentHashMap<String, LongAdder> acceptedBySymbol = new ConcurrentHashMap<>();

    private volatile IntSupplier queueSizeSupplier = () -> 0;
    private volatile IntSupplier queueCapacitySupplier = () -> 0;
    private volatile Supplier<Boolean> degradedSupplier = () -> false;
    private final long startedAtMillis;

    public ServiceMetrics() {
        this.startedAtMillis = System.currentTimeMillis();
    }

    /** 在写入服务构造完成后绑定队列/降级指标来源，避免 Bean 循环依赖。 */
    public void bindObservables(IntSupplier queueSizeSupplier,
                                IntSupplier queueCapacitySupplier,
                                Supplier<Boolean> degradedSupplier) {
        this.queueSizeSupplier = queueSizeSupplier;
        this.queueCapacitySupplier = queueCapacitySupplier;
        this.degradedSupplier = degradedSupplier;
    }

    public void recordReceived() {
        received.increment();
    }

    /** 记录一条在线处理结论（恢复路径不经过这里，避免把历史恢复计入在线吞吐）。 */
    public void recordResult(String symbol, IngestionResult result) {
        if (result.outcome() == IngestionOutcome.ACCEPTED) {
            accepted.increment();
            acceptedBySymbol.computeIfAbsent(symbol, k -> new LongAdder()).increment();
        } else if (result.outcome() == IngestionOutcome.DUPLICATE) {
            duplicate.increment();
        } else {
            rejected.increment();
            incrementReason(result.reason());
        }
    }

    public void recordRejected(RejectReason reason) {
        rejected.increment();
        incrementReason(reason);
    }

    private void incrementReason(RejectReason reason) {
        switch (reason) {
            case INVALID_DATA -> rejectedInvalid.increment();
            case STALE -> rejectedStale.increment();
            case CLOCK_ROLLBACK -> rejectedClockRollback.increment();
            case BACKPRESSURE -> rejectedBackpressure.increment();
            default -> { }
        }
    }

    public void recordRecovered(long count) {
        recovered.add(count);
    }

    public long getReceived() {
        return received.sum();
    }

    public MetricsSnapshot snapshot() {
        Map<String, Long> bySymbol = new LinkedHashMap<>();
        acceptedBySymbol.forEach((k, v) -> bySymbol.put(k, v.sum()));
        return new MetricsSnapshot(
                received.sum(),
                accepted.sum(),
                duplicate.sum(),
                rejected.sum(),
                rejectedInvalid.sum(),
                rejectedStale.sum(),
                rejectedClockRollback.sum(),
                rejectedBackpressure.sum(),
                queueCapacitySupplier.getAsInt(),
                queueSizeSupplier.getAsInt(),
                recovered.sum(),
                degradedSupplier.get(),
                System.currentTimeMillis() - startedAtMillis,
                bySymbol);
    }
}
