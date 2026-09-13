package com.kaan9898.notificationservice.consumer;

import com.kaan9898.notificationservice.dto.OrderCreatedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {
    @KafkaListener(topics = "order-created", groupId = "notification-service")
    public void consume(OrderCreatedEvent orderCreatedEvent){
        System.out.println("Order sent to customer : ");
        System.out.println("CustomerId : " + orderCreatedEvent.customerId());
        System.out.println("OrderId: " + orderCreatedEvent.orderId());
        System.out.println("Product: " + orderCreatedEvent.product());
    }
}
