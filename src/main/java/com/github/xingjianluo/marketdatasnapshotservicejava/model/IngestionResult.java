package com.github.xingjianluo.marketdatasnapshotservicejava.model;

/**
 * 单条推送的处理结果。
 *
 * @param outcome 总体结论
 * @param reason  非接受原因（ACCEPTED 时为 {@link RejectReason#NONE}）
 * @param detail  判定依据的可读说明
 */
public record IngestionResult(IngestionOutcome outcome, RejectReason reason, String detail) {

    public static IngestionResult accepted(String detail) {
        return new IngestionResult(IngestionOutcome.ACCEPTED, RejectReason.NONE, detail);
    }

    public static IngestionResult duplicate(String detail) {
        return new IngestionResult(IngestionOutcome.DUPLICATE, RejectReason.NONE, detail);
    }

    public static IngestionResult rejected(RejectReason reason, String detail) {
        return new IngestionResult(IngestionOutcome.REJECTED, reason, detail);
    }

    public boolean accepted() {
        return outcome == IngestionOutcome.ACCEPTED;
    }
}
