package com.hmdp.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class LocalCacheConfig {

    @Bean("localDataCache")
    public Cache<String, String> localDataCache(
            @Value("${hmdp.cache.local.maximum-size:10000}") long maximumSize,
            @Value("${hmdp.cache.local.expire-after-write-seconds:30}") long expireAfterWriteSeconds) {
        return Caffeine.newBuilder()
                .maximumSize(maximumSize)
                .expireAfterWrite(expireAfterWriteSeconds, TimeUnit.SECONDS)
                .recordStats()
                .build();
    }

    @Bean("localNullCache")
    public Cache<String, Boolean> localNullCache(
            @Value("${hmdp.cache.local.null-expire-after-write-seconds:5}") long expireAfterWriteSeconds) {
        return Caffeine.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(expireAfterWriteSeconds, TimeUnit.SECONDS)
                .build();
    }
}
