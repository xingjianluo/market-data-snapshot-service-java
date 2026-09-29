package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 写入判定结果。accepted=false 时 reason 给出可区分的拒绝原因。
 */
public record IngestResult(boolean accepted, RejectReason reason, String detail,
                           long serverReceiveTime, Quote quote) {

    public static IngestResult rejected(RejectReason reason, String detail, long serverTime) {
        return new IngestResult(false, reason, detail, serverTime, null);
    }
}
