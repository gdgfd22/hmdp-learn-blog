package com.hmdp.cache;

import com.hmdp.utils.CacheClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
public class CacheInvalidationSubscriber implements MessageListener {

    private final CacheClient cacheClient;

    public CacheInvalidationSubscriber(CacheClient cacheClient) {
        this.cacheClient = cacheClient;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String key = new String(message.getBody(), StandardCharsets.UTF_8);
        if (key.trim().isEmpty()) {
            return;
        }
        cacheClient.evictLocal(key);
        log.debug("local cache invalidated by broadcast, key={}", key);
    }
}
