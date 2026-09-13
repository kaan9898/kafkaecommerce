package com.kaan9898.inventoryservice.consumer;

import com.kaan9898.inventoryservice.dto.OrderCreatedEvent;
import com.kaan9898.inventoryservice.dto.InventoryResultEvent;
import com.kaan9898.inventoryservice.entity.InventoryStatus;
import com.kaan9898.inventoryservice.producer.InventoryProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OrderConsumer {
    private final InventoryProducer inventoryProducer;
    private final Set<UUID> processedOrders = ConcurrentHashMap.newKeySet();

    public OrderConsumer(InventoryProducer inventoryProducer) {
        this.inventoryProducer = inventoryProducer;
    }

    @KafkaListener(topics = "order-created",groupId = "inventory-service")
    public void consume(OrderCreatedEvent orderCreatedEvent) {
        System.out.println("Processing order: ");
        System.out.println("OrderId: "+ orderCreatedEvent.orderId());
        System.out.println("Product: "+ orderCreatedEvent.product());
        System.out.println("Quantity: "+ orderCreatedEvent.quantity());
        if(processedOrders.contains(orderCreatedEvent.orderId())) {
            System.out.println("Duplicate order ignored: " + orderCreatedEvent.orderId());
            return;
        }
        if(new Random().nextBoolean()) {
            System.out.println("Random error occurred: ");
            throw new RuntimeException("Random inventory processing error");
        }
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
        processedOrders.add(orderCreatedEvent.orderId());
        System.out.println("Inventory Result: " + status);
    }
}
