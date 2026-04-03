package com.hmdp.mq;

import com.hmdp.dto.SocialNotificationMessage;
import com.hmdp.entity.Notification;
import com.hmdp.service.INotificationService;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.NotificationConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Slf4j
@Component
public class SocialNotificationConsumer {

    @Resource
    private INotificationService notificationService;

    @RabbitListener(queues = MqConstants.SOCIAL_NOTIFICATION_QUEUE)
    public void handleSocialNotification(SocialNotificationMessage message) {
        if (message == null || message.getReceiverUserId() == null) {
            return;
        }
        Notification notification = new Notification();
        notification.setUserId(message.getReceiverUserId());
        notification.setSenderUserId(message.getSenderUserId());
        notification.setType(message.getType());
        notification.setBizId(message.getBizId());
        notification.setContent(message.getContent());
        notification.setReadStatus(NotificationConstants.READ_UNREAD);
        notificationService.save(notification);
        log.info("social notification persisted, receiverUserId={}, type={}, bizId={}",
                message.getReceiverUserId(), message.getType(), message.getBizId());
    }
}
