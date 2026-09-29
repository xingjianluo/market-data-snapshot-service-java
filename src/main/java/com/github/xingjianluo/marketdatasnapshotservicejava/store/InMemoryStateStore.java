package com.github.xingjianluo.marketdatasnapshotservicejava.store;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内品种状态存储。
 *
 * <p>{@link #getOrCreate} 通过 {@link ConcurrentHashMap#computeIfAbsent} 保证每个品种
 * 只有一个 {@link SymbolState} 实例，并发写入与并发查询拿到同一把锁。
 * 每个品种的复合状态变更都在其 SymbolState 自身的监视器锁内完成。
 */
public final class InMemoryStateStore {

    private final ConcurrentHashMap<String, SymbolState> bySymbol = new ConcurrentHashMap<>();
    private final int dedupCacheCapacity;
    private final long windowLengthMillis;

    public InMemoryStateStore(int dedupCacheCapacity, long windowLengthMillis) {
        this.dedupCacheCapacity = dedupCacheCapacity;
        this.windowLengthMillis = windowLengthMillis;
    }

    public SymbolState getOrCreate(String symbol) {
        return bySymbol.computeIfAbsent(symbol,
                s -> new SymbolState(s, dedupCacheCapacity, windowLengthMillis));
    }

    public SymbolState get(String symbol) {
        return bySymbol.get(symbol);
    }

    public List<String> symbols() {
        return bySymbol.keySet().stream().sorted().toList();
    }
}
