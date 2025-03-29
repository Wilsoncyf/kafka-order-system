package com.example.kafkaordersystem.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    @KafkaListener(topics = "test-topic", groupId = "order-group")
    public void listen(ConsumerRecord<String, String> record) {
        System.out.println("📥 Received message: " + record.value());
    }
}
