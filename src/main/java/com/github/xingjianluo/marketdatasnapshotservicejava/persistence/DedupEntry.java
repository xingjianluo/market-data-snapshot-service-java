package com.github.xingjianluo.marketdatasnapshotservicejava.persistence;

/**
 * 检查点中的去重条目。
 */
public record DedupEntry(String eventId, long eventTime) {
}
