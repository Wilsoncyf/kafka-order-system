package com.example.kafkaordersystem.controller;

import com.example.kafkaordersystem.kafka.ProducerService;
import com.example.kafkaordersystem.model.OrderMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final ProducerService producerService;

    @PostMapping("/create")
    public String createOrder(@RequestParam String orderId,
                              @RequestParam String productId,
                              @RequestParam int quantity) {
        OrderMessage order = new OrderMessage(orderId, productId, quantity);
        producerService.sendOrder(order);
        return "✅ 订单消息已发送：" + order;
    }
}
