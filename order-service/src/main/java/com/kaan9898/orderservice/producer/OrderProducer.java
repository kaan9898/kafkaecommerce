package com.kaan9898.orderservice.producer;

import com.kaan9898.orderservice.dto.OrderCreatedEvent;
import jakarta.annotation.Resource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducer {
    private static final String TOPIC = "order-created";
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    public void sendOrderCreatedEvent(OrderCreatedEvent orderCreatedEvent) {
        kafkaTemplate.send(TOPIC, orderCreatedEvent.orderId().toString(), orderCreatedEvent);
    }

}
