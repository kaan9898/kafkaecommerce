package com.kaan9898.orderservice.consumer;

import com.kaan9898.orderservice.dto.InventoryResultEvent;
import com.kaan9898.orderservice.service.OrderService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class InventoryResultConsumer {
    private final OrderService orderService;

    public InventoryResultConsumer(OrderService orderService) {
        this.orderService = orderService;
    }
    @KafkaListener(
            topics = "inventory-result", groupId = "order-service"
    )
    public void receiveInventoryResultEvent(InventoryResultEvent event) {
        orderService.updateOrderStatus(event);
    }
}
