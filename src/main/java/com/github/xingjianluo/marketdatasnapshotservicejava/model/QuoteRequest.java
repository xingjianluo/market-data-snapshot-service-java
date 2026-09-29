package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 客户端推送的原始行情请求，接收时间由服务端加盖。
 */
public record QuoteRequest(String eventId, String symbol, Long eventTime, Long price, Long size) {
}
