package com.hmdp.mq;

import com.hmdp.dto.SocialNotificationMessage;
import com.hmdp.utils.MqConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;

@Slf4j
@Component
public class SocialNotificationProducer {

    @Resource
    private RabbitTemplate rabbitTemplate;

    public void publishAfterCommit(SocialNotificationMessage message) {
        if (message == null || message.getReceiverUserId() == null || message.getSenderUserId() == null) {
            return;
        }
        if (message.getReceiverUserId().equals(message.getSenderUserId())) {
            return;
        }
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send(message);
                }
            });
            return;
        }
        send(message);
    }

    private void send(SocialNotificationMessage message) {
        try {
            rabbitTemplate.convertAndSend(
                    MqConstants.SOCIAL_NOTIFICATION_EXCHANGE,
                    MqConstants.SOCIAL_NOTIFICATION_ROUTING_KEY,
                    message
            );
        } catch (Exception e) {
            log.error("Failed to publish social notification, message={}", message, e);
        }
    }
}
