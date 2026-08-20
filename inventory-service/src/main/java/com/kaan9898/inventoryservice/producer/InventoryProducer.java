package com.kaan9898.inventoryservice.producer;

import com.kaan9898.inventoryservice.dto.InventoryResultEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryProducer {
    private static final String TOPIC = "inventory-result";
    private final KafkaTemplate<String, InventoryResultEvent> kafkaTemplate;

    public InventoryProducer(KafkaTemplate<String, InventoryResultEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    public void sendInventoryResultEvent(InventoryResultEvent event) {
        kafkaTemplate.send(TOPIC, event.orderId().toString(), event);
    }
}
