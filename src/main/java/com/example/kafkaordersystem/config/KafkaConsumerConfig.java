package com.example.kafkaordersystem.config;

import com.example.kafkaordersystem.exception.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.function.BiFunction;

@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        // 重试间隔配置（2秒间隔，最多重试2次）
        FixedBackOff backOff = new FixedBackOff(2000L, 2);

        // 路由逻辑：根据异常类型动态决定目标死信 Topic
        BiFunction<ConsumerRecord<?, ?>, Exception, String> destinationResolver = (record, ex) -> {
            Throwable cause = ex.getCause();
            String topic;

            if (cause instanceof NetworkException) {
                topic = "network-error-topic";
            } else if (cause instanceof DataFormatException) {
                topic = "invalid-order-topic";
            } else if (cause instanceof BusinessException) {
                topic = "notify-dev-topic";
            } else {
                topic = "generic-dead-letter-topic";
            }

            // 👇 发送通知消息（可用独立 Topic 进行统一处理）
            String alertMsg = String.format("⚠️ Kafka异常路由\n异常类型: %s\nTopic: %s\n消息内容: %s",
                    cause.getClass().getSimpleName(), topic, record.value());
            kafkaTemplate.send("alert-topic", alertMsg);

            return topic;
        };


        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(destinationResolver.apply(record, ex), record.partition())
        );

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.setRetryListeners((record, ex, attempt) -> {
            System.out.println("❌ 第 " + attempt + " 次重试失败: " + record.value());
        });

        return errorHandler;
    }
}
