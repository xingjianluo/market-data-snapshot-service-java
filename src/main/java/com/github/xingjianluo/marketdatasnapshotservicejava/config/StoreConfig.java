package com.github.xingjianluo.marketdatasnapshotservicejava.config;

import com.github.xingjianluo.marketdatasnapshotservicejava.store.QuoteStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StoreConfig {

    @Bean
    public QuoteStore quoteStore(ServiceProperties properties) {
        return new QuoteStore(properties.getWindowSizeMillis(), properties.getRetainedWindows());
    }
}
