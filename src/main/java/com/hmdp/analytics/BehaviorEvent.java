package com.hmdp.analytics;

import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;

@Data
@Accessors(chain = true)
@JsonNaming(PropertyNamingStrategy.SnakeCaseStrategy.class)
public class BehaviorEvent {

    private String eventId;
    private String eventType;
    private Long userId;
    private String deviceId;
    private Long shopId;
    private Long blogId;
    private Long voucherId;
    private Long orderId;
    private String result;
    private Long eventTime;
    private Long ingestTime;
    private Map<String, String> properties = new HashMap<>();

    public static BehaviorEvent of(String eventType) {
        return new BehaviorEvent().setEventType(eventType);
    }

    public BehaviorEvent addProperty(String name, Object value) {
        if (name != null && value != null) {
            properties.put(name, String.valueOf(value));
        }
        return this;
    }
}
