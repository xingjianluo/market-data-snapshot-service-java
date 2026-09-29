package com.github.xingjianluo.marketdatasnapshotservicejava.engine;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.IngestionResult;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.SnapshotView;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowAggregate;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.InMemoryStateStore;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.SymbolState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 核心判定处理器。预期只由单个消费者线程（在线处理）或启动恢复线程调用，
 * 但所有判定与更新都在对应 {@link SymbolState} 的锁内完成，并发快照查询不会读到半更新。
 *
 * <h2>判定规则（按顺序）</h2>
 * <ol>
 *   <li><b>非法数据 INVALID_DATA</b>：symbol/source 空白，price/size 非有限正数，
 *       seqNum 为负，事件时间为负或超过墙钟未来容忍。非法数据不参与任何状态，也不写日志文件。</li>
 *   <li><b>重复 DUPLICATE</b>：(source, seqNum) 已在该品种去重缓存中。重复消息不再计入任何统计聚合。</li>
 *   <li><b>时钟回拨 CLOCK_ROLLBACK</b>：事件时间比本品种已见最大事件时间落后超过
 *       {@code clockRollbackThresholdMillis}。与"可解释的乱序"区分：回拨代表来源时钟异常/串源。</li>
 *   <li><b>过期 STALE</b>：事件时间落后最大事件时间，但差距在回拨阈值之内、
 *       又超过 {@code disorderToleranceMillis} 乱序容忍窗口——无法作为可归位的轻微乱序接受。</li>
 *   <li><b>接受 ACCEPTED</b>：首条消息，或落后不超过乱序容忍窗口的乱序消息（归位），
 *       或时间更新的消息。最新快照与窗口聚合都按事件时间归位。</li>
 * </ol>
 * 注意："过期"是来源数据相对于已见序列的判定；快照上的 stale 标志是相对于墙钟的数据新鲜度判定，二者不同。
 */
public class OrderedProcessor {

    private static final Logger log = LoggerFactory.getLogger(OrderedProcessor.class);

    /** 允许事件时间领先墙钟的上限，超过视为非法（时钟错误），与回拨方向相反、互相区分。 */
    static final long FUTURE_TOLERANCE_MILLIS = 5_000L;

    private final InMemoryStateStore store;
    private final ServiceProperties properties;
    private final ServiceMetrics metrics;

    public OrderedProcessor(InMemoryStateStore store, ServiceProperties properties, ServiceMetrics metrics) {
        this.store = store;
        this.properties = properties;
        this.metrics = metrics;
    }

    /**
     * 处理一条报价。在线与恢复共用同一套幂等路径；recovery=true 时不重复计入在线指标。
     */
    public IngestionResult process(Quote quote, boolean recovery) {
        String phase = recovery ? "recovery" : "online";
        IngestionResult invalid = validate(quote);
        if (invalid != null) {
            log.info("quote rejected phase={} reason=INVALID_DATA quote={} basis={}",
                    phase, quote, invalid.detail());
            return invalid;
        }

        SymbolState state = store.getOrCreate(quote.symbol());
        IngestionResult result;
        synchronized (state) {
            if (!state.markSeen(quote.dedupKey())) {
                result = IngestionResult.duplicate(
                        "dedupKey already seen: " + quote.dedupKey());
                log.info("quote duplicate phase={} quote={} basis={}", phase, quote, result.detail());
                return result;
            }

            boolean first = state.getMaxEventTimeMillis() == Long.MIN_VALUE;
            long gap = first ? 0L : state.getMaxEventTimeMillis() - quote.eventTimeMillis();

            if (!first && gap > properties.getClockRollbackThresholdMillis()) {
                result = IngestionResult.rejected(
                        com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.CLOCK_ROLLBACK,
                        String.format(
                                "eventTime %,d is %,d ms behind maxEventTime %,d, exceeds clock-rollback threshold %,d ms",
                                quote.eventTimeMillis(), gap, state.getMaxEventTimeMillis(),
                                properties.getClockRollbackThresholdMillis()));
            } else if (!first && gap > properties.getDisorderToleranceMillis()) {
                result = IngestionResult.rejected(
                        com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.STALE,
                        String.format(
                                "eventTime %,d is %,d ms behind maxEventTime %,d, exceeds disorder tolerance %,d ms (within rollback threshold %,d ms)",
                                quote.eventTimeMillis(), gap, state.getMaxEventTimeMillis(),
                                properties.getDisorderToleranceMillis(),
                                properties.getClockRollbackThresholdMillis()));
            } else {
                state.accept(quote);
                String position;
                if (first) {
                    position = "first quote for symbol";
                } else if (gap < 0) {
                    position = "newer than current max by " + (-gap) + " ms, advances timeline";
                } else if (gap == 0) {
                    position = "same eventTime as current max, distinct message accepted";
                } else {
                    position = "out-of-order within tolerance, placed by eventTime (" + gap + " ms late)";
                }
                result = IngestionResult.accepted(position);
            }
        }

        switch (result.outcome()) {
            case ACCEPTED -> log.info(
                    "quote accepted phase={} quote={} basis={}", phase, quote, result.detail());
            case REJECTED -> log.info(
                    "quote rejected phase={} reason={} quote={} basis={}",
                    phase, result.reason(), quote, result.detail());
            default -> { }
        }
        return result;
    }

    private IngestionResult validate(Quote q) {
        if (q == null) {
            return IngestionResult.rejected(
                    com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.INVALID_DATA,
                    "payload is null");
        }
        if (q.symbol() == null || q.symbol().isBlank()) {
            return IngestionResult.rejected(
                    com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.INVALID_DATA,
                    "symbol is blank");
        }
        if (q.source() == null || q.source().isBlank()) {
            return IngestionResult.rejected(
                    com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.INVALID_DATA,
                    "source is blank");
        }
        if (q.seqNum() < 0) {
            return IngestionResult.rejected(
                    com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.INVALID_DATA,
                    "seqNum must be non-negative: " + q.seqNum());
        }
        if (!Double.isFinite(q.price()) || q.price() <= 0d) {
            return IngestionResult.rejected(
                    com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.INVALID_DATA,
                    "price must be a finite positive number: " + q.price());
        }
        if (!Double.isFinite(q.size()) || q.size() <= 0d) {
            return IngestionResult.rejected(
                    com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.INVALID_DATA,
                    "size must be a finite positive number: " + q.size());
        }
        if (q.eventTimeMillis() < 0L) {
            return IngestionResult.rejected(
                    com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.INVALID_DATA,
                    "eventTimeMillis must be non-negative: " + q.eventTimeMillis());
        }
        long now = System.currentTimeMillis();
        if (q.eventTimeMillis() > now + FUTURE_TOLERANCE_MILLIS) {
            return IngestionResult.rejected(
                    com.github.xingjianluo.marketdatasnapshotservicejava.model.RejectReason.INVALID_DATA,
                    String.format("eventTimeMillis %,d is more than %,d ms in the future (now=%,d)",
                            q.eventTimeMillis(), FUTURE_TOLERANCE_MILLIS, now));
        }
        return null;
    }

    /**
     * 构造某品种当前一致快照：在品种锁内复制出不可变视图后立即释放锁，
     * 后续遍历/序列化无需持锁，写入者也不会被查询阻塞。
     */
    public SnapshotView snapshot(String symbol, boolean degraded) {
        SymbolState state = store.get(symbol);
        // 注册表项可能在写入线程放入状态对象后、接受首条数据前被读者观察到；
        // 尚未接受任何报价的品种与未知品种一样返回 null（HTTP 层映射为 404）。
        if (state == null) {
            return null;
        }
        Quote latestCopy;
        long maxEventTime;
        WindowAggregate aggregate;
        long accepted;
        synchronized (state) {
            if (!state.hasData()) {
                return null;
            }
            Quote l = state.getLatest();
            latestCopy = l == null ? null : new Quote(l.symbol(), l.source(), l.seqNum(),
                    l.eventTimeMillis(), l.price(), l.size(), l.receivedAtMillis());
            maxEventTime = state.getMaxEventTimeMillis();
            aggregate = state.currentAggregate();
            accepted = state.getAcceptedCount();
        }
        long staleness = Math.max(0L, System.currentTimeMillis() - maxEventTime);
        boolean stale = staleness > properties.getFreshnessToleranceMillis();
        String trustNote = buildTrustNote(stale, degraded, staleness, accepted, aggregate);
        return new SnapshotView(symbol, latestCopy, maxEventTime, aggregate,
                stale, degraded, staleness, trustNote);
    }

    private String buildTrustNote(boolean stale, boolean degraded, long staleness,
                                  long accepted, WindowAggregate aggregate) {
        if (accepted == 0) {
            return "NO_DATA: no accepted quote yet, snapshot is not trustworthy for trading decisions";
        }
        if (degraded && stale) {
            return String.format(
                    "UNTRUSTED: service degraded under backpressure and data is %,d ms stale",
                    staleness);
        }
        if (degraded) {
            return "DEGRADED: recent writes were rejected due to backpressure; snapshot may lag";
        }
        if (stale) {
            return String.format(
                    "STALE: latest event time is %,d ms older than wall clock (tolerance %,d ms)",
                    staleness, properties.getFreshnessToleranceMillis());
        }
        return "OK: fresh and not degraded";
    }

    public long acceptedCount(String symbol) {
        SymbolState state = store.get(symbol);
        if (state == null) {
            return 0L;
        }
        synchronized (state) {
            return state.getAcceptedCount();
        }
    }
}
