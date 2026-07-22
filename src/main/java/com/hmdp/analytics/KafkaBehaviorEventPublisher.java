package com.hmdp.analytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.UserDTO;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@ConditionalOnProperty(name = "hmdp.analytics.events.enabled", havingValue = "true")
public class KafkaBehaviorEventPublisher implements BehaviorEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public KafkaBehaviorEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                       ObjectMapper objectMapper,
                                       @Value("${hmdp.analytics.events.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Override
    public void publish(BehaviorEvent event) {
        if (event == null || event.getEventType() == null) {
            return;
        }
        long now = System.currentTimeMillis();
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getEventTime() == null) {
            event.setEventTime(now);
        }
        event.setIngestTime(now);
        UserDTO user = UserHolder.getUser();
        if (event.getUserId() == null && user != null) {
            event.setUserId(user.getId());
        }
        try {
            String key = event.getUserId() == null ? event.getDeviceId() : event.getUserId().toString();
            kafkaTemplate.send(topic, key == null ? event.getEventId() : key, objectMapper.writeValueAsString(event))
                    .addCallback(
                            result -> log.debug("behavior event published, type={}, eventId={}", event.getEventType(), event.getEventId()),
                            ex -> log.error("behavior event publish failed, type={}, eventId={}", event.getEventType(), event.getEventId(), ex));
        } catch (JsonProcessingException e) {
            log.error("behavior event serialization failed, type={}", event.getEventType(), e);
        }
    }
}
