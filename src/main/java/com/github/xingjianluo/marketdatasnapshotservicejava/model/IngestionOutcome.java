package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/** 一条推送经过处理通道后的总体结论。 */
public enum IngestionOutcome {
    ACCEPTED,
    DUPLICATE,
    REJECTED
}
