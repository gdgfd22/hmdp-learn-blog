package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.github.benmanes.caffeine.cache.Cache;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_INVALIDATION_CHANNEL;
import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;
    private final Cache<String, String> localDataCache;
    private final Cache<String, Boolean> localNullCache;
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(
            StringRedisTemplate stringRedisTemplate,
            @Qualifier("localDataCache") Cache<String, String> localDataCache,
            @Qualifier("localNullCache") Cache<String, Boolean> localNullCache) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.localDataCache = localDataCache;
        this.localNullCache = localNullCache;
    }

    public void set(String key, Object value,Long time, TimeUnit unit) {
        String json = JSONUtil.toJsonStr(value);
        stringRedisTemplate.opsForValue().set(key, json, time, unit);
        putLocalData(key, json);
    }

    /**
     * 设置逻辑过期
     */
    public void setWithLogicalExpire(String key, Object value,Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        String json = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(key, json);
        putLocalData(key, json);
    }

    /**
     * 使用缓存空对象是为了解决缓存穿透
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbfallback
     * @param time
     * @param unit
     * @return
     * @param <R>
     * @param <ID>
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix,ID id,Class<R> type, Function<ID,R> dbfallback,Long time, TimeUnit unit) {
        //Function<ID,R> dbfallback,一个函数，接收 ID，返回 R
        String key = keyPrefix + id;
        // 1.依次查询Caffeine L1和Redis L2
        String json = getCachedJson(key);
        // 2.判断是否存在
        if(StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json,type);
        }
        //判断命中的是否是空值
        if(json != null) {
            return null;
        }
        //4.不存在，根据id查询数据库
        R r = dbfallback.apply(id);
        //5.不存在，返回错误
        if(r == null){
            //将空值写入两级缓存
            cacheNull(key);
            //返回错误信息
            return null;
        }
        //6.存在，写入两级缓存
        this.set(key,r,time,unit);
        return r;
    }

    /**
     * 逻辑过期解决缓存击穿
     * @param keyPrefix
     * @param id
     * @param type
     * @param dbFallback
     * @param time
     * @param unit
     * @return
     * @param <R>
     * @param <ID>
     */
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.依次查询Caffeine L1和Redis L2
        String json = getCachedJson(key);
        // 2.判断是否存在
        if (StrUtil.isBlank(json)) {
            // 3.不存在，直接返回
            return null;
        }
        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 5.1.未过期，直接返回店铺信息
            return r;
        }
        // 5.2.已过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2.判断是否获取锁成功
        if (isLock){
            // 6.3.成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库
                    R newR = dbFallback.apply(id);
                    if (newR == null) {
                        cacheNull(key);
                    } else {
                        // 重建两级缓存
                        this.setWithLogicalExpire(key, newR, time, unit);
                    }
                } catch (Exception e) {
                    log.error("failed to rebuild logical-expire cache, key={}", key, e);
                }finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }
        // 6.4.返回过期的商铺信息
        return r;
    }

    public <R, ID> R queryWithMutex(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.依次查询Caffeine L1和Redis L2
        String shopJson = getCachedJson(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson, type);
        }
        // 判断命中的是否是空值
        if (shopJson != null) {
            // 返回一个错误信息
            return null;
        }

        // 4.实现缓存重建
        // 4.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        R r = null;
        boolean lockAcquired = false;
        try {
            lockAcquired = tryLock(lockKey);
            // 4.2.判断是否获取成功
            if (!lockAcquired) {
                // 4.3.获取锁失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }
            // 4.4.获取锁后双重检查，避免等待锁期间其他线程已经完成缓存重建
            String latestJson = getCachedJson(key);
            if (StrUtil.isNotBlank(latestJson)) {
                return JSONUtil.toBean(latestJson, type);
            }
            if (latestJson != null) {
                return null;
            }
            // 4.4.获取锁成功，根据id查询数据库
            r = dbFallback.apply(id);
            // 5.不存在，返回错误
            if (r == null) {
                // 将空值写入两级缓存
                cacheNull(key);
                // 返回错误信息
                return null;
            }
            // 6.存在，写入两级缓存
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while rebuilding cache", e);
        }finally {
            // 7.释放锁
            if (lockAcquired) {
                unlock(lockKey);
            }
        }
        // 8.返回
        return r;
    }

    /**
     * 数据库事务提交后再失效缓存，避免事务回滚时误删缓存。
     */
    public void invalidateAfterCommit(String key) {
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    invalidate(key);
                }
            });
            return;
        }
        invalidate(key);
    }

    /**
     * 删除共享L2并广播消息，使所有应用实例清理自己的Caffeine L1。
     */
    public void invalidate(String key) {
        evictLocal(key);
        stringRedisTemplate.delete(key);
        stringRedisTemplate.convertAndSend(CACHE_INVALIDATION_CHANNEL, key);
    }

    public void evictLocal(String key) {
        localDataCache.invalidate(key);
        localNullCache.invalidate(key);
    }

    private String getCachedJson(String key) {
        String localJson = localDataCache.getIfPresent(key);
        if (localJson != null) {
            return localJson;
        }
        if (Boolean.TRUE.equals(localNullCache.getIfPresent(key))) {
            return "";
        }

        String redisJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(redisJson)) {
            putLocalData(key, redisJson);
            return redisJson;
        }
        if (redisJson != null) {
            localNullCache.put(key, Boolean.TRUE);
            return "";
        }
        return null;
    }

    private void putLocalData(String key, String json) {
        localNullCache.invalidate(key);
        localDataCache.put(key, json);
    }

    private void cacheNull(String key) {
        stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
        localDataCache.invalidate(key);
        localNullCache.put(key, Boolean.TRUE);
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
