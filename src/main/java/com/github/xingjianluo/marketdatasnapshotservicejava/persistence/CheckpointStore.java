package com.github.xingjianluo.marketdatasnapshotservicejava.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * 检查点读写（{@code stateDir/checkpoint.json}）。
 *
 * <p>写入使用“临时文件 + 原子 move”，崩溃只会留下旧检查点或新检查点，
 * 不会读到半文件。恢复流程：先装检查点（含去重索引），再重放 WAL，
 * 因此无论重复执行多少次恢复，同一条历史行情只累计一次。
 */
@Component
public class CheckpointStore {

    private static final Logger log = LoggerFactory.getLogger(CheckpointStore.class);
    public static final String CHECKPOINT_FILE = "checkpoint.json";

    private final Path stateDir;
    private final ObjectMapper mapper;

    public CheckpointStore(ServiceProperties properties) {
        this.stateDir = Path.of(properties.getStateDir());
        this.mapper = new ObjectMapper();
    }

    public void save(CheckpointState state) {
        try {
            Files.createDirectories(stateDir);
            Path target = stateDir.resolve(CHECKPOINT_FILE);
            Path tmp = stateDir.resolve(CHECKPOINT_FILE + ".tmp");
            byte[] body = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
            Files.write(tmp, body);
            try {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            log.info("Checkpoint saved: createdAt={}, symbols={}",
                    state.createdAt(), state.latestBySymbol().size());
        } catch (IOException e) {
            throw new UncheckedIOException("checkpoint save failed", e);
        }
    }

    public Optional<CheckpointState> load() {
        Path target = stateDir.resolve(CHECKPOINT_FILE);
        if (!Files.exists(target)) {
            return Optional.empty();
        }
        try {
            CheckpointState state = mapper.readValue(Files.readAllBytes(target),
                    CheckpointState.class);
            log.info("Checkpoint loaded: createdAt={}, symbols={}",
                    state.createdAt(), state.latestBySymbol().size());
            return Optional.of(state);
        } catch (IOException e) {
            throw new UncheckedIOException("checkpoint load failed", e);
        }
    }
}
