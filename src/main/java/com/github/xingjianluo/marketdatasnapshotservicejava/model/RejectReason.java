package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 拒绝/非接受原因，各类原因必须可区分：
 * NONE                - 无（被接受）
 * INVALID_DATA        - 非法数据（字段缺失、价格数量非正、时间戳非法等）
 * STALE               - 过期：乱序超出可归位容忍窗口
 * CLOCK_ROLLBACK      - 时钟回拨：事件时间落后本品种已见最大时间超过回拨阈值
 * BACKPRESSURE        - 背压：有界队列已满，显式拒绝
 */
public enum RejectReason {
    NONE,
    INVALID_DATA,
    STALE,
    CLOCK_ROLLBACK,
    BACKPRESSURE
}
