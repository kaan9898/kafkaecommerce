package com.kaan9898.inventoryservice.consumer;

import com.kaan9898.inventoryservice.dto.OrderCreatedEvent;
import com.kaan9898.inventoryservice.dto.InventoryResultEvent;
import com.kaan9898.inventoryservice.entity.InventoryStatus;
import com.kaan9898.inventoryservice.producer.InventoryProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {
    private final InventoryProducer inventoryProducer;

    public OrderConsumer(InventoryProducer inventoryProducer) {
        this.inventoryProducer = inventoryProducer;
    }

    @KafkaListener(topics = "order-created",groupId = "inventory-service")
    public void consume(OrderCreatedEvent orderCreatedEvent) {
        System.out.println("Processing order: ");
        System.out.println("OrderId: "+ orderCreatedEvent.orderId());
        System.out.println("Product: "+ orderCreatedEvent.product());
        System.out.println("Quantity: "+ orderCreatedEvent.quantity());
        InventoryStatus status;
        if(orderCreatedEvent.quantity()<=5){
            status = InventoryStatus.AVAILABLE;
        }
        else{
            status = InventoryStatus.OUT_OF_STOCK;
        }
        InventoryResultEvent result = new InventoryResultEvent(
                orderCreatedEvent.orderId(),
                status
        );
        inventoryProducer.sendInventoryResultEvent(result);
        System.out.println("Inventory Result: " + status);
    }
}
