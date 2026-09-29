package com.github.xingjianluo.marketdatasnapshotservicejava.dedup;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 按品种持有 {@link DedupIndex}。处理线程与恢复线程通过同一注册表拿到同一索引，
 * 保证“检查点去重条目 + 运行期去重条目”对所有判定路径一致可见。
 */
@Component
public class DedupIndexRegistry {

    private final int retainedPerSymbol;
    private final ConcurrentHashMap<String, DedupIndex> indexes = new ConcurrentHashMap<>();

    public DedupIndexRegistry(ServiceProperties properties) {
        this.retainedPerSymbol = properties.getDedupRetainedPerSymbol();
    }

    public DedupIndex indexFor(String symbol) {
        return indexes.computeIfAbsent(symbol, s -> new DedupIndex(retainedPerSymbol));
    }

    public ConcurrentHashMap<String, DedupIndex> all() {
        return indexes;
    }
}
