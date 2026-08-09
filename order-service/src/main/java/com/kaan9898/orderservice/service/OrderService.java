package com.kaan9898.orderservice.service;

import com.kaan9898.orderservice.dto.OrderCreatedEvent;
import com.kaan9898.orderservice.dto.OrderRequest;
import com.kaan9898.orderservice.producer.OrderProducer;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {
    private final OrderProducer orderProducer;
    public OrderService(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }
    public OrderCreatedEvent createOrder(OrderRequest request) {
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                UUID.randomUUID(),
                request.customerId(),
                request.product(),
                request.quantity(),
                Instant.now()
        );
        orderProducer.sendOrderCreatedEvent(orderCreatedEvent);
        return orderCreatedEvent;
    }
}
