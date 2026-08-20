package com.hmdp.utils;

import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hmdp.entity.Shop;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hmdp.utils.RedisConstants.CACHE_INVALIDATION_CHANNEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CacheClientTest {

    private StringRedisTemplate redisTemplate;
    private ValueOperations<String, String> valueOperations;
    private Cache<String, String> localDataCache;
    private Cache<String, Boolean> localNullCache;
    private CacheClient cacheClient;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        redisTemplate = mock(StringRedisTemplate.class);
        valueOperations = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        localDataCache = Caffeine.newBuilder().build();
        localNullCache = Caffeine.newBuilder().build();
        cacheClient = new CacheClient(redisTemplate, localDataCache, localNullCache);
    }

    @Test
    void shouldReturnFromCaffeineWithoutAccessingRedisOrDatabase() {
        String key = "cache:shop:1";
        Shop cachedShop = shop(1L, "local shop");
        localDataCache.put(key, JSONUtil.toJsonStr(cachedShop));
        AtomicInteger databaseCalls = new AtomicInteger();

        Shop result = cacheClient.queryWithPassThrough(
                "cache:shop:", 1L, Shop.class,
                id -> {
                    databaseCalls.incrementAndGet();
                    return null;
                },
                30L, TimeUnit.MINUTES);

        assertEquals("local shop", result.getName());
        assertEquals(0, databaseCalls.get());
        verify(valueOperations, never()).get(anyString());
    }

    @Test
    void shouldWarmCaffeineAfterRedisHit() {
        String key = "cache:shop:2";
        when(valueOperations.get(key)).thenReturn(JSONUtil.toJsonStr(shop(2L, "redis shop")));

        Shop first = queryShop(2L, id -> null);
        Shop second = queryShop(2L, id -> null);

        assertEquals("redis shop", first.getName());
        assertEquals("redis shop", second.getName());
        verify(valueOperations, times(1)).get(key);
        assertTrue(localDataCache.getIfPresent(key) != null);
    }

    @Test
    void shouldPopulateBothLevelsAfterDatabaseFallback() {
        String key = "cache:shop:3";
        when(valueOperations.get(key)).thenReturn(null);

        Shop result = queryShop(3L, id -> shop(id, "database shop"));

        assertEquals("database shop", result.getName());
        verify(valueOperations).set(
                org.mockito.ArgumentMatchers.eq(key),
                anyString(),
                org.mockito.ArgumentMatchers.eq(30L),
                org.mockito.ArgumentMatchers.eq(TimeUnit.MINUTES));
        assertTrue(localDataCache.getIfPresent(key) != null);
        assertNull(localNullCache.getIfPresent(key));
    }

    @Test
    void shouldCacheNullValueLocallyAfterRedisNullSentinelHit() {
        String key = "cache:shop:404";
        when(valueOperations.get(key)).thenReturn("");
        AtomicInteger databaseCalls = new AtomicInteger();

        Shop first = queryShop(404L, id -> {
            databaseCalls.incrementAndGet();
            return null;
        });
        Shop second = queryShop(404L, id -> {
            databaseCalls.incrementAndGet();
            return null;
        });

        assertNull(first);
        assertNull(second);
        assertEquals(0, databaseCalls.get());
        verify(valueOperations, times(1)).get(key);
        assertEquals(Boolean.TRUE, localNullCache.getIfPresent(key));
    }

    @Test
    void shouldInvalidateBothLevelsAndPublishBroadcast() {
        String key = "cache:shop:5";
        localDataCache.put(key, JSONUtil.toJsonStr(shop(5L, "old shop")));

        cacheClient.invalidate(key);

        assertNull(localDataCache.getIfPresent(key));
        assertNull(localNullCache.getIfPresent(key));
        verify(redisTemplate).delete(key);
        verify(redisTemplate).convertAndSend(CACHE_INVALIDATION_CHANNEL, key);
    }

    @Test
    void shouldDelayInvalidationUntilTransactionCommit() {
        String key = "cache:shop:6";
        localDataCache.put(key, JSONUtil.toJsonStr(shop(6L, "old shop")));
        TransactionSynchronizationManager.setActualTransactionActive(true);
        TransactionSynchronizationManager.initSynchronization();
        try {
            cacheClient.invalidateAfterCommit(key);

            assertTrue(localDataCache.getIfPresent(key) != null);
            verify(redisTemplate, never()).delete(key);
            List<TransactionSynchronization> synchronizations =
                    TransactionSynchronizationManager.getSynchronizations();
            assertEquals(1, synchronizations.size());

            synchronizations.get(0).afterCommit();

            assertFalse(localDataCache.asMap().containsKey(key));
            verify(redisTemplate).delete(key);
            verify(redisTemplate).convertAndSend(CACHE_INVALIDATION_CHANNEL, key);
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    private Shop queryShop(Long id, java.util.function.Function<Long, Shop> databaseFallback) {
        return cacheClient.queryWithPassThrough(
                "cache:shop:", id, Shop.class, databaseFallback, 30L, TimeUnit.MINUTES);
    }

    private Shop shop(Long id, String name) {
        Shop shop = new Shop();
        shop.setId(id);
        shop.setName(name);
        return shop;
    }
}
