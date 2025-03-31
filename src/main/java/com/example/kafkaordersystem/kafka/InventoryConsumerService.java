package com.example.kafkaordersystem.kafka;

import com.example.kafkaordersystem.exception.BusinessException;
import com.example.kafkaordersystem.exception.DataFormatException;
import com.example.kafkaordersystem.exception.NetworkException;
import com.example.kafkaordersystem.model.OrderMessage;
import com.example.kafkaordersystem.util.IdempotencyUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumerService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private IdempotencyUtil idempotencyUtil;

    @KafkaListener(topics = "test-topic", groupId = "inventory-group")
    public void consume(ConsumerRecord<String, String> record) throws JsonProcessingException {
        try {
            OrderMessage order = objectMapper.readValue(record.value(), OrderMessage.class);

            // 👇 幂等校验
            if (!idempotencyUtil.markIfNotProcessed("inventory",order.getOrderId())) {
                System.out.println("⚠️ [库存服务] 已处理过订单：" + order.getOrderId() + "，跳过处理");
                return;
            }

            if ("network".equals(order.getOrderId())) {
                throw new NetworkException("模拟网络异常");
            } else if ("data".equals(order.getOrderId())) {
                throw new DataFormatException("模拟数据异常");
            } else if ("business".equals(order.getOrderId())) {
                throw new BusinessException("模拟业务异常");
            }


            System.out.println("📦 [库存服务] 扣减商品库存，订单号: " + order.getOrderId()
                    + "，商品: " + order.getProductId()
                    + "，数量: " + order.getQuantity());
        } catch (Exception e) {
            throw e; // 抛出异常才能触发 retry 和死信处理
        }
    }

}
