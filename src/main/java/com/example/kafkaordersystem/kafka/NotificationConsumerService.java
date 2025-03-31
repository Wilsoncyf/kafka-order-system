package com.example.kafkaordersystem.kafka;

import com.example.kafkaordersystem.model.OrderMessage;
import com.example.kafkaordersystem.util.IdempotencyUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumerService {

    @Autowired
    private IdempotencyUtil idempotencyUtil;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "test-topic", groupId = "notification-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            OrderMessage order = objectMapper.readValue(record.value(), OrderMessage.class);

            // 幂等校验（模块名：notification）
            if (!idempotencyUtil.markIfNotProcessed("notification", order.getOrderId())) {
                System.out.println("⚠️ [通知服务] 订单已通知过，跳过 orderId=" + order.getOrderId());
                return;
            }
            System.out.println("📩 [通知服务] 已发送短信/邮件给用户：订单号：" + order.getOrderId());
        } catch (Exception e) {
            System.err.println("❌ [通知服务] 消息解析失败：" + record.value());
        }
    }
}
