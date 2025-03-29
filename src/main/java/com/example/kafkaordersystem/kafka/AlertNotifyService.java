package com.example.kafkaordersystem.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class AlertNotifyService {

    private final RestTemplate restTemplate = new RestTemplate();

    @KafkaListener(topics = "alert-topic", groupId = "alert-group")
    public void consume(String message) {
        System.out.println("📢 [报警通知] 收到异常消息: " + message);

        // 模拟发钉钉群机器人通知
        sendToDingTalk(message);
    }

    private void sendToDingTalk(String msg) {
//        String webhook = "https://oapi.dingtalk.com/robot/send?access_token=你自己的token";
//
//        String payload = String.format("{\"msgtype\": \"text\", \"text\": {\"content\": \"%s\"}}", msg);
//
//        try {
//            restTemplate.postForObject(webhook, payload, String.class);
//            System.out.println("✅ 钉钉通知已发送");
//        } catch (Exception e) {
//            System.out.println("❌ 钉钉通知失败：" + e.getMessage());
//        }
        System.out.println("✅ 钉钉通知已发送:" + msg);
    }
}
