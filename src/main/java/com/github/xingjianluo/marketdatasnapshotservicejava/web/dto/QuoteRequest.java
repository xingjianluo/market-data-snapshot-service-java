package com.github.xingjianluo.marketdatasnapshotservicejava.web.dto;

/** 推送请求体。source/seqNum/eventTimeMillis 可选：缺省时由服务端补默认值。 */
public record QuoteRequest(
        String symbol,
        String source,
        Long seqNum,
        Long eventTimeMillis,
        Double price,
        Double size) {
}
