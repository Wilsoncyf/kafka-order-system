# Kafka Order System 🛒

一个基于 Spring Boot + Kafka 的电商订单系统：

## 功能亮点：

- ✅ Kafka 消息生产/消费
- ✅ 消费失败重试机制（可配置次数）
- ✅ 按异常分类路由到多个 DLT（死信队列）
- ✅ KafkaTemplate 发送钉钉告警通知
- ✅ 自动补偿服务重试死信消息
- ✅ Kafka UI 可视化集成
- ✅ 使用 Docker Compose 部署 Kafka 全链路

## 模块结构：

- ProducerService：订单消息生产
- InventoryConsumerService：扣库存逻辑
- NotificationConsumerService：发通知逻辑
- AlertNotifyService：钉钉通知
- AutoCompensationService：死信补偿
- KafkaConsumerConfig：重试策略配置
