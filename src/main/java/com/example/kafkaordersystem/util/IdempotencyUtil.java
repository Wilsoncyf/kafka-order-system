package com.example.kafkaordersystem.util;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class IdempotencyUtil {

    private final StringRedisTemplate redisTemplate;

    private static final String KEY_PREFIX = "kafka:processed";

    /**
     * 幂等校验：不同模块使用不同 key
     * @param module 消费模块名（如 inventory / notification）
     * @param uniqueId 唯一业务ID（如 orderId）
     * @return true = 第一次处理，false = 已处理过
     */
    public boolean markIfNotProcessed(String module, String uniqueId) {
        String key = String.format("%s:%s:%s", KEY_PREFIX, module, uniqueId);

        Boolean success = redisTemplate.opsForValue().setIfAbsent(key, "1", 1, TimeUnit.DAYS);
        return Boolean.TRUE.equals(success);
    }
}
