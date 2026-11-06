package com.kaan9898.notificationservice.consumer;

import com.kaan9898.notificationservice.dto.OrderEvent;
import com.kaan9898.notificationservice.dto.OrderEventType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {
    @KafkaListener(topics = "order-events", groupId = "notification-service")
    public void consume(OrderEvent orderEvent){
        if(orderEvent.eventType() == OrderEventType.ORDER_CREATED){
            System.out.println("Order Created: " + orderEvent.orderId() + "CorrelationId: " + orderEvent.correlationId());
            System.out.println("Email sent to customer: " + orderEvent.customerId());
        }
        if(orderEvent.eventType() == OrderEventType.ORDER_UPDATED){
            System.out.println("Order Updated: " + orderEvent.orderId());
        }
        if(orderEvent.eventType() == OrderEventType.ORDER_CANCELLED){
            System.out.println("Order Cancelled: " + orderEvent.orderId());
        }
    }
}
