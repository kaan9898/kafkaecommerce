package com.kaan9898.inventoryservice.consumer;

import com.kaan9898.inventoryservice.dto.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DltConsumer {
    @KafkaListener(topics = "order-created.DLT",groupId = "dlt-monitor")
    public void dltConsumer(OrderCreatedEvent orderCreatedEvent) {
        System.out.println("Failed message from DLT");
        System.out.println("OrderId: " + orderCreatedEvent.orderId());
        System.out.println("Product: " + orderCreatedEvent.product());
        System.out.println("Quantity: " + orderCreatedEvent.quantity());
    }
}
