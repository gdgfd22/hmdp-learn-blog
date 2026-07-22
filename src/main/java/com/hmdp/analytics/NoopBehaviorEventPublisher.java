package com.hmdp.analytics;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "hmdp.analytics.events.enabled", havingValue = "false", matchIfMissing = true)
public class NoopBehaviorEventPublisher implements BehaviorEventPublisher {

    @Override
    public void publish(BehaviorEvent event) {
        // Analytics is optional for the business application and disabled by default.
    }
}
