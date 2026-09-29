package com.github.xingjianluo.marketdatasnapshotservicejava.persistence;

import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.WindowStats;

import java.util.List;
import java.util.Map;

/**
 * 一次一致性检查点的完整状态：最新价、窗口聚合、去重索引。
 * 恢复后再重放 WAL，去重索引保证不重复累计。
 */
public record CheckpointState(long createdAt,
                              Map<String, Quote> latestBySymbol,
                              Map<String, List<WindowStats>> windowsBySymbol,
                              Map<String, List<DedupEntry>> dedupBySymbol) {
}
