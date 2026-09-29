package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 一条行情报价。不可变。
 *
 * @param symbol           品种代码
 * @param source           行情源标识
 * @param seqNum           行情源内单调递增的序列号
 * @param eventTimeMillis  行情事件时间（epoch millis）
 * @param price            价格
 * @param size             数量
 * @param receivedAtMillis 服务端接收时间（epoch millis）
 */
public record Quote(
        String symbol,
        String source,
        long seqNum,
        long eventTimeMillis,
        double price,
        double size,
        long receivedAtMillis) {

    /** 去重键：同一行情源内同一序列号即视为同一消息。 */
    public String dedupKey() {
        return source + "#" + seqNum;
    }
}
