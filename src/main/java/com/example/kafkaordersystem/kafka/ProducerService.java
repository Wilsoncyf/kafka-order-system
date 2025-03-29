package com.example.kafkaordersystem.kafka;

import com.example.kafkaordersystem.model.OrderMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void sendOrder(OrderMessage orderMessage) {
        try {
            String json = objectMapper.writeValueAsString(orderMessage);
            kafkaTemplate.send("test-topic", json);
            System.out.println("✅ Sent JSON message: " + json);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }
}
