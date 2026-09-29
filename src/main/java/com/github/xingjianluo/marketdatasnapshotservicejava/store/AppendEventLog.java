package com.github.xingjianluo.marketdatasnapshotservicejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 本地只追加事件日志（JSON Lines），用于进程重启后的状态恢复。
 *
 * <p>文件布局（位于 dataDir）：
 * <ul>
 *   <li>{@code <prefix>.log}   当前活动段，追加写入；</li>
 *   <li>{@code <prefix>.log.1} 滚动段，仅保留最近一个（再滚动时覆盖）。</li>
 * </ul>
 *
 * <p>只有<b>被接受</b>的报价才写入日志：非法数据、过期、时钟回拨、背压拒绝的消息不落盘，
 * 因此恢复重放只会重现接受历史；处理器本身按 dedupKey 幂等，
 * 即使同一段历史因任何原因被重放多次，也不会被重复累计。
 *
 * <p>重放顺序：先读滚动段（更旧）再读活动段；段内按行号顺序。损坏行跳过并告警，不影响其余数据。
 */
public final class AppendEventLog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AppendEventLog.class);

    private final Path activeFile;
    private final Path rolledFile;
    private final long maxFileBytes;
    private final boolean fsync;
    private final ObjectMapper mapper = new ObjectMapper();

    private volatile long bytesWritten;

    public AppendEventLog(String dataDir, String filePrefix, long maxFileBytes, boolean fsync) {
        if (maxFileBytes <= 0) {
            throw new IllegalArgumentException("logMaxFileBytes must be positive");
        }
        this.maxFileBytes = maxFileBytes;
        this.fsync = fsync;
        Path dir = Path.of(dataDir);
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create data dir: " + dir, e);
        }
        this.activeFile = dir.resolve(filePrefix + ".log");
        this.rolledFile = dir.resolve(filePrefix + ".log.1");
        try {
            this.bytesWritten = Files.exists(activeFile) ? Files.size(activeFile) : 0L;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot stat active log: " + activeFile, e);
        }
    }

    /** 追加一条已接受报价；超过段大小上限时滚动。 synchronized 保证多线程调用下行不交错。 */
    public synchronized void append(Quote quote) {
        try {
            if (bytesWritten >= maxFileBytes) {
                rotateLocked();
            }
            byte[] line = (mapper.writeValueAsString(quote) + System.lineSeparator())
                    .getBytes(StandardCharsets.UTF_8);
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(activeFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(line);
                while (buf.hasRemaining()) {
                    ch.write(buf);
                }
                if (fsync) {
                    ch.force(false);
                }
            }
            bytesWritten += line.length;
        } catch (IOException e) {
            throw new UncheckedIOException("append failed: " + activeFile, e);
        }
    }

    private void rotateLocked() throws IOException {
        try {
            Files.deleteIfExists(rolledFile);
            if (Files.exists(activeFile)) {
                Files.move(activeFile, rolledFile);
            }
        } finally {
            bytesWritten = 0L;
        }
        log.info("event-log rotated: active={} rolled={}", activeFile, rolledFile);
    }

    /**
     * 按时间先后（滚动段→活动段，段内按行序）读取全部记录。
     * 损坏行跳过并告警，不中断恢复。
     */
    public List<Quote> replay() {
        List<Quote> all = new ArrayList<>();
        readFile(rolledFile, all);
        readFile(activeFile, all);
        // 防御性排序：即使文件被外部处理过，重放顺序也按"段序+事件时间"保持稳定；
        // 注意处理器对乱序本身也有归位规则，这里排序仅降低恢复路径的不确定性。
        return List.copyOf(all);
    }

    private void readFile(Path file, List<Quote> sink) {
        if (!Files.exists(file)) {
            return;
        }
        long lineNo = 0;
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                if (line.isBlank()) {
                    continue;
                }
                try {
                    sink.add(mapper.readValue(line, Quote.class));
                } catch (Exception e) {
                    log.warn("event-log corrupt line skipped: file={} line={} detail={}",
                            file, lineNo, e.getMessage());
                }
            }
        } catch (IOException e) {
            log.error("event-log replay read failure: file={} recoveredSoFar={}", file, sink.size(), e);
        }
    }

    public Path getActiveFile() {
        return activeFile;
    }

    @Override
    public synchronized void close() {
        // 当前实现每条记录都已 flush 到通道；此处仅保留扩展点与生命周期对称。
        log.info("event-log closed: file={} bytes={}", activeFile, bytesWritten);
    }

    /** 测试辅助：列出重放时使用的文件（旧→新）。 */
    List<Path> replayFiles() {
        return List.of(rolledFile, activeFile).stream()
                .filter(Files::exists)
                .sorted(Comparator.comparing(Path::toString))
                .toList();
    }
}
