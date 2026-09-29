package com.github.xingjianluo.marketdatasnapshotservicejava.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 追加式 WAL（{@code stateDir/wal.log}，JSON Lines）。
 *
 * <ul>
 *   <li>每条 accepted 行情在内存状态更新之前同步 append+flush，保证崩溃后可重放；</li>
 *   <li>检查点固化内存后调用 {@link #rotate()} 删除已覆盖日志；</li>
 *   <li>恢复由 {@link RecoveryService} 顺序读取当前 wal.log，重复条目被去重索引拦截。</li>
 * </ul>
 * 所有方法 synchronized：append 只被单处理线程调用，rotate 亦然，但保留串行保证。
 */
@Component
public class WalWriter implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WalWriter.class);
    public static final String WAL_FILE = "wal.log";

    private final Path stateDir;
    private final ObjectMapper mapper = new ObjectMapper();
    private BufferedOutputStream out;

    public WalWriter(ServiceProperties properties) {
        this.stateDir = Path.of(properties.getStateDir());
    }

    public synchronized void open() {
        try {
            Files.createDirectories(stateDir);
            Path wal = stateDir.resolve(WAL_FILE);
            this.out = new BufferedOutputStream(Files.newOutputStream(wal,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND));
            log.info("WAL opened at {}", wal.toAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open WAL", e);
        }
    }

    public synchronized void append(Quote quote) {
        if (out == null) {
            throw new IllegalStateException("WAL not opened");
        }
        try {
            byte[] line = mapper.writeValueAsBytes(quote);
            out.write(line);
            out.write('\n');
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("WAL append failed for event " + quote.eventId(), e);
        }
    }

    /** 检查点成功后调用：关闭并删除已被检查点覆盖的 WAL。 */
    public synchronized void rotate() {
        if (out != null) {
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                log.warn("WAL close before rotate failed", e);
            }
            out = null;
        }
        try {
            Files.deleteIfExists(stateDir.resolve(WAL_FILE));
        } catch (IOException e) {
            throw new UncheckedIOException("WAL rotate failed", e);
        }
        Path wal = stateDir.resolve(WAL_FILE);
        try {
            this.out = new BufferedOutputStream(Files.newOutputStream(wal,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND));
        } catch (IOException e) {
            throw new UncheckedIOException("WAL reopen failed", e);
        }
        log.info("WAL rotated (old entries covered by checkpoint)");
    }

    public Path walPath() {
        return stateDir.resolve(WAL_FILE);
    }

    @PreDestroy
    @Override
    public synchronized void close() {
        if (out != null) {
            try {
                out.flush();
                out.close();
            } catch (IOException e) {
                log.warn("WAL close failed", e);
            } finally {
                out = null;
            }
        }
    }
}
