package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 一条已被服务端接收的行情（不可变）。
 * 价格/数量使用 long 整数档位，避免浮点不确定性。
 */
public record Quote(String eventId, String symbol, long eventTime, long price, long size) {
}
