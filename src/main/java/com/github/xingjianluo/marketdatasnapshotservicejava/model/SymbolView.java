package com.github.xingjianluo.marketdatasnapshotservicejava.model;

import java.util.List;

/**
 * 某品种在同一时点上的一致快照：最新价 + 窗口聚合来自同一临界区。
 */
public record SymbolView(String symbol, Quote latest, boolean stale,
                         long dataAgeMillis, long windowSizeMillis,
                         List<WindowStats> windows) {
}
