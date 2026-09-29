package com.github.xingjianluo.marketdatasnapshotservicejava.config;

import com.github.xingjianluo.marketdatasnapshotservicejava.engine.OrderedProcessor;
import com.github.xingjianluo.marketdatasnapshotservicejava.engine.ServiceMetrics;
import com.github.xingjianluo.marketdatasnapshotservicejava.ingest.QueueingIngestService;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.AppendEventLog;
import com.github.xingjianluo.marketdatasnapshotservicejava.store.InMemoryStateStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Bean 装配。QueueingIngestService 用 @Bean 显式创建，
 * 保证其 {@code @PostConstruct} 启动（含日志恢复）发生在模拟器与 Web 层就绪之前。
 */
@Configuration
@EnableConfigurationProperties(ServiceProperties.class)
public class ServiceConfiguration {

    @Bean
    public InMemoryStateStore inMemoryStateStore(ServiceProperties properties) {
        return new InMemoryStateStore(properties.getDedupCacheCapacity(),
                properties.getWindowLengthMillis());
    }

    @Bean
    public ServiceMetrics serviceMetrics() {
        return new ServiceMetrics();
    }

    @Bean
    public OrderedProcessor orderedProcessor(InMemoryStateStore store,
                                             ServiceProperties properties,
                                             ServiceMetrics metrics) {
        return new OrderedProcessor(store, properties, metrics);
    }

    @Bean
    public AppendEventLog appendEventLog(ServiceProperties properties) {
        return new AppendEventLog(properties.getDataDir(), properties.getLogFilePrefix(),
                properties.getLogMaxFileBytes(), properties.isLogFsync());
    }

    @Bean
    public QueueingIngestService queueingIngestService(ServiceProperties properties,
                                                       InMemoryStateStore store,
                                                       OrderedProcessor processor,
                                                       ServiceMetrics metrics,
                                                       AppendEventLog eventLog) {
        QueueingIngestService service =
                new QueueingIngestService(properties, store, processor, metrics, eventLog);
        metrics.bindObservables(service::queueSize, service::queueCapacity, service::isDegraded);
        return service;
    }
}
