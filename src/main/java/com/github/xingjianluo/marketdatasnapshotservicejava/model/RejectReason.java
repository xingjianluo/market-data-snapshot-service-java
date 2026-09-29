package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 一条行情推送的判定结果类别。
 * ACCEPTED / OUT_OF_ORDER 表示计入；其余表示拒绝或服务状态。
 */
public enum RejectReason {
    ACCEPTED,
    OUT_OF_ORDER,
    DUPLICATE,
    INVALID_DATA,
    EXPIRED,
    CLOCK_ROLLBACK,
    BACKPRESSURE,
    SERVICE_CLOSED,
    PERSISTENCE_FAILURE
}
