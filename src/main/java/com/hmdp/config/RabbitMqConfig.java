package com.hmdp.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.utils.MqConstants;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMqConfig {

    @Bean
    public DirectExchange socialNotificationExchange() {
        return new DirectExchange(MqConstants.SOCIAL_NOTIFICATION_EXCHANGE, true, false);
    }

    @Bean
    public Queue socialNotificationQueue() {
        return QueueBuilder.durable(MqConstants.SOCIAL_NOTIFICATION_QUEUE).build();
    }

    @Bean
    public Binding socialNotificationBinding(Queue socialNotificationQueue,
                                             DirectExchange socialNotificationExchange) {
        return BindingBuilder.bind(socialNotificationQueue)
                .to(socialNotificationExchange)
                .with(MqConstants.SOCIAL_NOTIFICATION_ROUTING_KEY);
    }

    @Bean
    public MessageConverter messageConverter(ObjectMapper objectMapper) {
        return new Jackson2JsonMessageConverter(objectMapper);
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(messageConverter);
        return rabbitTemplate;
    }
}
