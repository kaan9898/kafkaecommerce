package com.kaan9898.inventoryservice.consumer;

import com.kaan9898.inventoryservice.dto.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {
    @KafkaListener(topics = "order-created",groupId = "inventory-service")
    public void consume(OrderCreatedEvent orderCreatedEvent) {
        System.out.println("Processing order: ");
        System.out.println("OrderId: "+ orderCreatedEvent.orderId());
        System.out.println("Product: "+ orderCreatedEvent.product());
        System.out.println("Quantity: "+ orderCreatedEvent.quantity());


    }
}
