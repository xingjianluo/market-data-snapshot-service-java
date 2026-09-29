package com.github.xingjianluo.marketdatasnapshotservicejava.dedup;

import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.DedupEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 单品种有界去重索引（每品种一个实例）。
 *
 * <ul>
 *   <li>判重键：eventId。同一 eventId 第二次出现即 DUPLICATE，永不二次计入。</li>
 *   <li>有界：超过 retained 时按 eventTime 从最旧开始淘汰，防止无界内存增长。</li>
 *   <li>可恢复：条目可导出到检查点并在启动时装载，因此恢复后去重仍然生效；
 *       WAL 重放遇到已在检查点中的 eventId 会被判重跳过，重复恢复不重复累计。</li>
 * </ul>
 * 非线程安全：每个实例仅被 IngestService 的单处理线程访问（恢复也在处理前完成）。
 */
public final class DedupIndex {

    private final int retained;
    private final Map<String, Long> eventTimeById = new HashMap<>();
    private final TreeMap<Long, LinkedHashSet<String>> idsByEventTime = new TreeMap<>();

    public DedupIndex(int retained) {
        if (retained <= 0) {
            throw new IllegalArgumentException("retained must be positive: " + retained);
        }
        this.retained = retained;
    }

    /**
     * @return true 表示新事件并已登记；false 表示重复事件，状态不变。
     */
    public boolean registerIfAbsent(String eventId, long eventTime) {
        if (eventTimeById.containsKey(eventId)) {
            return false;
        }
        eventTimeById.put(eventId, eventTime);
        idsByEventTime.computeIfAbsent(eventTime, t -> new LinkedHashSet<>()).add(eventId);
        evictIfNeeded();
        return true;
    }

    /** 只查询不登记（恢复阶段判定 WAL 是否已包含在检查点中）。 */
    public boolean contains(String eventId) {
        return eventTimeById.containsKey(eventId);
    }

    private void evictIfNeeded() {
        while (eventTimeById.size() > retained) {
            Map.Entry<Long, LinkedHashSet<String>> oldest = idsByEventTime.firstEntry();
            LinkedHashSet<String> ids = oldest.getValue();
            String victim = ids.iterator().next();
            ids.remove(victim);
            if (ids.isEmpty()) {
                idsByEventTime.remove(oldest.getKey());
            }
            eventTimeById.remove(victim);
        }
    }

    /** 丢弃 eventTime &lt;= cutoff 的条目（恢复重放前可按水位清理）。 */
    public void pruneOlderThan(long cutoff) {
        while (!idsByEventTime.isEmpty() && idsByEventTime.firstKey() <= cutoff) {
            LinkedHashSet<String> ids = idsByEventTime.pollFirstEntry().getValue();
            for (String id : ids) {
                eventTimeById.remove(id);
            }
        }
    }

    /** 检查点导出。 */
    public List<DedupEntry> entries() {
        List<DedupEntry> result = new ArrayList<>(eventTimeById.size());
        for (Map.Entry<String, Long> e : eventTimeById.entrySet()) {
            result.add(new DedupEntry(e.getKey(), e.getValue()));
        }
        return result;
    }

    /** 检查点/恢复装载。 */
    public void load(List<DedupEntry> entries) {
        if (entries == null) {
            return;
        }
        for (DedupEntry e : entries) {
            if (!eventTimeById.containsKey(e.eventId())) {
                eventTimeById.put(e.eventId(), e.eventTime());
                idsByEventTime.computeIfAbsent(e.eventTime(), t -> new LinkedHashSet<>())
                        .add(e.eventId());
            }
        }
        evictIfNeeded();
    }

    public int size() {
        return eventTimeById.size();
    }
}
