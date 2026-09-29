package com.github.xingjianluo.marketdatasnapshotservicejava.store;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 有界去重缓存（LRU）。容量内精确去重；
 * 容量外最久未使用的键会被淘汰——这是容量约束下的预期行为，容量可通过配置观察与调整。
 *
 * <p>本类自身线程安全（所有方法 synchronized）；在本服务中它只被单消费者线程访问，
 * 加锁仅为防御性措施与恢复阶段使用。
 */
public final class DedupCache {

    private final int capacity;
    private final Map<String, Boolean> keys;

    public DedupCache(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("dedupCacheCapacity must be positive: " + capacity);
        }
        this.capacity = capacity;
        this.keys = Collections.synchronizedMap(new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > DedupCache.this.capacity;
            }
        });
    }

    /**
     * 键不存在则加入并返回 true（首次见到）；已存在返回 false（重复）。
     */
    public boolean addIfAbsent(String key) {
        synchronized (keys) {
            if (keys.containsKey(key)) {
                return false;
            }
            keys.put(key, Boolean.TRUE);
            return true;
        }
    }

    public int size() {
        return keys.size();
    }

    public int capacity() {
        return capacity;
    }
}
