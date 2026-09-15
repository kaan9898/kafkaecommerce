package com.kaan9898.orderservice.producer;

import com.kaan9898.orderservice.dto.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class OrderProducer {
    private static final String TOPIC = "order-events";
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    public void sendEvent(OrderEvent orderEvent) {
        kafkaTemplate.send(TOPIC, orderEvent.customerId().toString(), orderEvent);
    }
}
