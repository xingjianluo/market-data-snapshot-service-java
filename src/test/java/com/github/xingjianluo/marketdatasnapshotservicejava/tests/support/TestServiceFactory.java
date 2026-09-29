package com.github.xingjianluo.marketdatasnapshotservicejava.tests.support;

import com.github.xingjianluo.marketdatasnapshotservicejava.config.ServiceProperties;
import com.github.xingjianluo.marketdatasnapshotservicejava.dedup.DedupIndexRegistry;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.IngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.CheckpointStore;
import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.RecoveryService;
import com.github.xingjianluo.marketdatasnapshotservicejava.persistence.WalWriter;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.QuoteStore;

import java.nio.file.Path;

/**
 * 不依赖 Spring 组装真实组件（WAL/恢复/单写线程全部是真实实现）。
 */
public final class TestServiceFactory {

    private TestServiceFactory() {
    }

    public static ServiceProperties props(Path stateDir) {
        ServiceProperties p = new ServiceProperties();
        p.setStateDir(stateDir.toString());
        return p;
    }

    public static IngestService newService(ServiceProperties props, MutableClock clock) {
        QuoteStore store = new QuoteStore(props.getWindowSizeMillis(), props.getRetainedWindows());
        WalWriter wal = new WalWriter(props);
        CheckpointStore cp = new CheckpointStore(props);
        DedupIndexRegistry registry = new DedupIndexRegistry(props);
        RecoveryService recovery = new RecoveryService(props, cp, wal, store, registry);
        return new IngestService(props, clock, store, wal, cp, recovery, registry);
    }

    public static IngestService started(ServiceProperties props, MutableClock clock) {
        IngestService svc = newService(props, clock);
        svc.start();
        return svc;
    }
}
