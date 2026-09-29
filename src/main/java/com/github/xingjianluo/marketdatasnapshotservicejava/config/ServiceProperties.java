package com.github.xingjianluo.marketdatasnapshotservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 全部可调配置项，前缀 market.service。
 */
@ConfigurationProperties(prefix = "market.service")
public class ServiceProperties {

    /** 写入队列容量，满则按背压策略拒绝。 */
    private int queueCapacity = 1024;
    /** offer 入队等待毫秒数；超时返回 BACKPRESSURE。 */
    private long enqueueTimeoutMillis = 50;
    /** 乱序容忍：eventTime 落后最新事件时间在该窗口内仍按乱序计入。 */
    private long outOfOrderLagMillis = 5_000;
    /** 超过乱序窗口但仍处于数据有效期内的消息判为 EXPIRED。 */
    private long dataTtlMillis = 60_000;
    /** 超过该年龄（相对服务端接收时间）判定为时钟回拨/时间不可信。 */
    private long clockRollbackAgeMillis = 24 * 60 * 60_000L;
    /** 聚合窗口大小。 */
    private long windowSizeMillis = 1_000;
    /** 每品种保留的窗口桶数量。 */
    private int retainedWindows = 60;
    /** 数据超过该年龄未更新则查询结果标记 stale（不可信）。 */
    private long staleThresholdMillis = 5_000;
    /** 去重集合保留的最近事件数（按时间淘汰）。 */
    private int dedupRetainedPerSymbol = 4096;
    /** WAL/检查点目录，留空使用工作目录下 market-data-state。 */
    private String stateDir = "market-data-state";
    /** 每处理多少条消息执行一次检查点。 */
    private long checkpointIntervalMessages = 1000;
    /** 两次检查点之间的最小间隔。 */
    private long checkpointIntervalMillis = 2_000;
    /** 启动时是否自动重放本地状态。 */
    private boolean recoveryEnabled = true;
    /** 是否启动内置模拟行情源。 */
    private boolean feedEnabled = false;
    /** 模拟源推送间隔。 */
    private long feedIntervalMillis = 20;
    /** 模拟源品种列表。 */
    private String feedSymbols = "BTCUSDT,ETHUSDT";
    /** 单条处理额外延迟（毫秒），默认 0；调大可确定性复现背压，仅演示/压测使用。 */
    private long processDelayMillis = 0;

    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public long getEnqueueTimeoutMillis() { return enqueueTimeoutMillis; }
    public void setEnqueueTimeoutMillis(long v) { this.enqueueTimeoutMillis = v; }
    public long getOutOfOrderLagMillis() { return outOfOrderLagMillis; }
    public void setOutOfOrderLagMillis(long v) { this.outOfOrderLagMillis = v; }
    public long getDataTtlMillis() { return dataTtlMillis; }
    public void setDataTtlMillis(long v) { this.dataTtlMillis = v; }
    public long getClockRollbackAgeMillis() { return clockRollbackAgeMillis; }
    public void setClockRollbackAgeMillis(long v) { this.clockRollbackAgeMillis = v; }
    public long getWindowSizeMillis() { return windowSizeMillis; }
    public void setWindowSizeMillis(long v) { this.windowSizeMillis = v; }
    public int getRetainedWindows() { return retainedWindows; }
    public void setRetainedWindows(int v) { this.retainedWindows = v; }
    public long getStaleThresholdMillis() { return staleThresholdMillis; }
    public void setStaleThresholdMillis(long v) { this.staleThresholdMillis = v; }
    public int getDedupRetainedPerSymbol() { return dedupRetainedPerSymbol; }
    public void setDedupRetainedPerSymbol(int v) { this.dedupRetainedPerSymbol = v; }
    public String getStateDir() { return stateDir; }
    public void setStateDir(String v) { this.stateDir = v; }
    public long getCheckpointIntervalMessages() { return checkpointIntervalMessages; }
    public void setCheckpointIntervalMessages(long v) { this.checkpointIntervalMessages = v; }
    public long getCheckpointIntervalMillis() { return checkpointIntervalMillis; }
    public void setCheckpointIntervalMillis(long v) { this.checkpointIntervalMillis = v; }
    public boolean isRecoveryEnabled() { return recoveryEnabled; }
    public void setRecoveryEnabled(boolean v) { this.recoveryEnabled = v; }
    public boolean isFeedEnabled() { return feedEnabled; }
    public void setFeedEnabled(boolean v) { this.feedEnabled = v; }
    public long getFeedIntervalMillis() { return feedIntervalMillis; }
    public void setFeedIntervalMillis(long v) { this.feedIntervalMillis = v; }
    public String getFeedSymbols() { return feedSymbols; }
    public void setFeedSymbols(String v) { this.feedSymbols = v; }
    public long getProcessDelayMillis() { return processDelayMillis; }
    public void setProcessDelayMillis(long v) { this.processDelayMillis = v; }
}
