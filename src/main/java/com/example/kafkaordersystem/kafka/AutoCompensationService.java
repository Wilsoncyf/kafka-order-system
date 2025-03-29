package com.example.kafkaordersystem.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Service
@RequiredArgsConstructor
public class AutoCompensationService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final int MAX_RETRY = 3;

    @Value("${compensation.enabled:true}")
    private boolean compensationEnabled;

    @KafkaListener(topics = "network-error-topic", groupId = "compensation-group")
    public void handleNetworkError(ConsumerRecord<String, String> record) {
        handleRetry("network-error-topic", record);
    }

    @KafkaListener(topics = "invalid-order-topic", groupId = "compensation-group")
    public void handleInvalidOrder(ConsumerRecord<String, String> record) {
        handleRetry("invalid-order-topic", record);
    }

    @KafkaListener(topics = "notify-dev-topic", groupId = "compensation-group")
    public void handleBusinessError(ConsumerRecord<String, String> record) {
        handleRetry("notify-dev-topic", record);
    }

    private void handleRetry(String sourceTopic, ConsumerRecord<String, String> record) {
        String value = record.value();
        if (!compensationEnabled) {
            System.out.println("🚫 [补偿关闭] 当前关闭状态，忽略补偿消息：" + value);
            return;
        }
        int currentRetry = getRetryCount(record);
        if (currentRetry >= MAX_RETRY) {
            System.out.println("🚫 [自动补偿] 超过最大补偿次数，丢弃消息：" + value);
            return;
        }

        int nextRetry = currentRetry + 1;

        // 构造新的 Header，记录 retry 次数
        RecordHeader retryHeader = new RecordHeader("retry-count",
                String.valueOf(nextRetry).getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send("test-topic", null, null, value, List.of(retryHeader).toString());
        System.out.printf("🔁 [自动补偿] 第 %d 次补偿成功: %s%n", nextRetry, value);
    }

    private int getRetryCount(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader("retry-count");
        if (header == null) return 0;

        try {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            return 0;
        }
    }
}
