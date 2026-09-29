package com.github.xingjianluo.marketdatasnapshotservicejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 服务配置项（前缀 market-data）。骨架仅声明类型。
 */
@ConfigurationProperties(prefix = "market-data")
public class ServiceProperties {

    private int queueCapacity = 1024;
    private long disorderToleranceMillis = 5_000L;
    private long clockRollbackThresholdMillis = 60_000L;
    private long freshnessToleranceMillis = 3_000L;
    private long windowLengthMillis = 60_000L;
    private int dedupCacheCapacity = 100_000;
    private boolean persistenceEnabled = true;
    private String dataDir = "./data";
    private String logFilePrefix = "quotes";
    private long logMaxFileBytes = 64L * 1024 * 1024;
    private boolean logFsync = false;
    private boolean simulatedFeedEnabled = true;
    private long simulatedFeedIntervalMillis = 100L;

    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int v) { this.queueCapacity = v; }
    public long getDisorderToleranceMillis() { return disorderToleranceMillis; }
    public void setDisorderToleranceMillis(long v) { this.disorderToleranceMillis = v; }
    public long getClockRollbackThresholdMillis() { return clockRollbackThresholdMillis; }
    public void setClockRollbackThresholdMillis(long v) { this.clockRollbackThresholdMillis = v; }
    public long getFreshnessToleranceMillis() { return freshnessToleranceMillis; }
    public void setFreshnessToleranceMillis(long v) { this.freshnessToleranceMillis = v; }
    public long getWindowLengthMillis() { return windowLengthMillis; }
    public void setWindowLengthMillis(long v) { this.windowLengthMillis = v; }
    public int getDedupCacheCapacity() { return dedupCacheCapacity; }
    public void setDedupCacheCapacity(int v) { this.dedupCacheCapacity = v; }
    public boolean isPersistenceEnabled() { return persistenceEnabled; }
    public void setPersistenceEnabled(boolean v) { this.persistenceEnabled = v; }
    public String getDataDir() { return dataDir; }
    public void setDataDir(String v) { this.dataDir = v; }
    public String getLogFilePrefix() { return logFilePrefix; }
    public void setLogFilePrefix(String v) { this.logFilePrefix = v; }
    public long getLogMaxFileBytes() { return logMaxFileBytes; }
    public void setLogMaxFileBytes(long v) { this.logMaxFileBytes = v; }
    public boolean isLogFsync() { return logFsync; }
    public void setLogFsync(boolean v) { this.logFsync = v; }
    public boolean isSimulatedFeedEnabled() { return simulatedFeedEnabled; }
    public void setSimulatedFeedEnabled(boolean v) { this.simulatedFeedEnabled = v; }
    public long getSimulatedFeedIntervalMillis() { return simulatedFeedIntervalMillis; }
    public void setSimulatedFeedIntervalMillis(long v) { this.simulatedFeedIntervalMillis = v; }
}
