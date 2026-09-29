package com.github.xingjianluo.marketdatasnapshotservicejava.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.dedup.DedupIndex;
import com.github.xingjianluo.marketdatasnapshotservicejava.dedup.DedupIndexRegistry;
import com.github.xingjianluo.marketdatasnapshotservicejava.model.Quote;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.QuoteStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * 启动恢复，必须在处理线程开始消费之前执行。
 *
 * <ol>
 *   <li>载入最近检查点：最新价、窗口聚合、去重索引快照；</li>
 *   <li>顺序重放 WAL 中检查点之后落盘的行情；</li>
 *   <li>每条行情先过 {@link DedupIndex}：检查点里已有的、或 WAL 内重复的，
 *       一律跳过，因此<b>重复执行恢复/重复恢复同一批历史数据不会重复累计</b>。</li>
 * </ol>
 */
@Component
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final ServiceProperties properties;
    private final CheckpointStore checkpointStore;
    private final WalWriter walWriter;
    private final QuoteStore store;
    private final DedupIndexRegistry dedupRegistry;
    private final ObjectMapper mapper = new ObjectMapper();

    public RecoveryService(ServiceProperties properties, CheckpointStore checkpointStore,
                           WalWriter walWriter, QuoteStore store,
                           DedupIndexRegistry dedupRegistry) {
        this.properties = properties;
        this.checkpointStore = checkpointStore;
        this.walWriter = walWriter;
        this.store = store;
        this.dedupRegistry = dedupRegistry;
    }

    /**
     * @return 去重后实际应用（计入窗口）的行情条数。
     */
    public long recover() {
        long checkpointAt = 0L;
        Optional<CheckpointState> cp = checkpointStore.load();
        if (cp.isPresent()) {
            CheckpointState state = cp.get();
            checkpointAt = state.createdAt();
            store.restore(state.latestBySymbol(), state.windowsBySymbol());
            state.dedupBySymbol().forEach((symbol, entries) ->
                    dedupRegistry.indexFor(symbol).load(entries));
            log.info("Recovery stage 1/2: checkpoint restored at {} (dedup entries per symbol loaded)",
                    checkpointAt);
        } else {
            log.info("Recovery stage 1/2: no checkpoint found");
        }

        long applied = 0L;
        long skippedDup = 0L;
        long malformed = 0L;
        Path wal = walWriter.walPath();
        if (Files.exists(wal)) {
            List<String> lines;
            try {
                lines = Files.readAllLines(wal, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new IllegalStateException("failed reading WAL for recovery", e);
            }
            for (String raw : lines) {
                if (raw.isBlank()) {
                    continue;
                }
                Quote quote;
                try {
                    quote = mapper.readValue(raw, Quote.class);
                } catch (IOException e) {
                    malformed++;
                    log.warn("Recovery: malformed WAL line skipped: {}", raw);
                    continue;
                }
                DedupIndex index = dedupRegistry.indexFor(quote.symbol());
                if (!index.registerIfAbsent(quote.eventId(), quote.eventTime())) {
                    skippedDup++;
                    log.info("Recovery duplicate skipped (not re-counted): eventId={}, symbol={}",
                            quote.eventId(), quote.symbol());
                    continue;
                }
                store.apply(quote);
                applied++;
            }
        }
        log.info("Recovery stage 2/2 complete: walReplayApplied={}, duplicatesSkipped={}, malformed={}",
                applied, skippedDup, malformed);
        return applied;
    }
}
