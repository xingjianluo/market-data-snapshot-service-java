package com.github.xingjianluo.marketdatasnapshotservicejava.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 启用配置属性绑定。
 */
@Configuration
@EnableConfigurationProperties(ServiceProperties.class)
public class ServiceConfig {
}
