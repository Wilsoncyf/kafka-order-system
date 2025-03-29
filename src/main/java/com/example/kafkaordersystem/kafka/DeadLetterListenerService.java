package com.example.kafkaordersystem.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class DeadLetterListenerService {

    @KafkaListener(topics = "test-topic-dlt", groupId = "dlt-handler-group")
    public void consumeDeadLetter(ConsumerRecord<String, String> record) {
        System.out.println("💀 [死信处理] 捕获失败消息： " + record.value());

        // TODO: 可以进行以下操作之一：
        // 1. 重新投递到原始 Topic：通过 kafkaTemplate.send("test-topic", record.value())
        // 2. 写入数据库进行人工审核
        // 3. 发邮件通知开发者
    }
}
