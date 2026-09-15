package com.kaan9898.inventoryservice.consumer;

import com.kaan9898.inventoryservice.dto.InventoryResultEvent;
import com.kaan9898.inventoryservice.dto.OrderEvent;
import com.kaan9898.inventoryservice.entity.InventoryStatus;
import com.kaan9898.inventoryservice.producer.InventoryProducer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
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

    @KafkaListener(topics = "order-events",groupId = "${spring.kafka.consumer.group-id}")
    public void consume(OrderEvent orderEvent, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        System.out.println("Processing order: ");
        System.out.println("OrderId: "+ orderEvent.orderId() + "CorrelationId: " + orderEvent.correlationId());
        System.out.println("Order Id: "+ orderEvent.customerId() + " Partition: "+ partition + " OrderId: " + orderEvent.orderId());
        System.out.println("Product: "+ orderEvent.product());
        System.out.println("Quantity: "+ orderEvent.quantity());
        if(processedOrders.contains(orderEvent.orderId())) {
            System.out.println("Duplicate order ignored: " + orderEvent.orderId());
            return;
        }
        if(new Random().nextBoolean()) {
            System.out.println("Random error occurred: ");
            throw new RuntimeException("Random inventory processing error");
        }
        InventoryStatus status;
        if(orderEvent.quantity()<=5){
            status = InventoryStatus.AVAILABLE;
        }
        else{
            status = InventoryStatus.OUT_OF_STOCK;
        }
        InventoryResultEvent result = new InventoryResultEvent(
                orderEvent.orderId(),
                status
        );
        inventoryProducer.sendInventoryResultEvent(result);
        processedOrders.add(orderEvent.orderId());
        System.out.println("Inventory Result: " + status);
    }
}
