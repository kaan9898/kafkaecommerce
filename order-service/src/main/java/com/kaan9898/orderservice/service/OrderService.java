package com.kaan9898.orderservice.service;

import com.kaan9898.orderservice.dto.InventoryResultEvent;
import com.kaan9898.orderservice.dto.OrderEvent;
import com.kaan9898.orderservice.dto.OrderEventType;
import com.kaan9898.orderservice.dto.OrderRequest;
import com.kaan9898.orderservice.entity.OrderEntity;
import com.kaan9898.orderservice.entity.OrderStatus;
import com.kaan9898.orderservice.producer.OrderProducer;
import com.kaan9898.orderservice.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class OrderService {
    private final OrderProducer orderProducer;
    private final OrderRepository orderRepository;
    public OrderService(OrderProducer orderProducer,  OrderRepository orderRepository) {
        this.orderProducer = orderProducer;
        this.orderRepository = orderRepository;
    }
    public OrderEvent createOrder(OrderRequest request) {
        UUID orderId = UUID.randomUUID();
        String correlationId = UUID.randomUUID().toString();
        LocalDateTime createdTime = LocalDateTime.now();
        OrderEntity orderEntity = new OrderEntity(
                orderId,
                request.customerId(),
                request.product(),
                request.quantity(),
                OrderStatus.NEW,
                createdTime
        );
        OrderEntity savedOrder = orderRepository.save(orderEntity);
        OrderEvent orderEvent = new OrderEvent(
                OrderEventType.ORDER_CREATED,
                savedOrder.getOrderId(),
                savedOrder.getCustomerId(),
                savedOrder.getProduct(),
                savedOrder.getQuantity(),
                savedOrder.getCreatedDate(),
                correlationId
        );
        System.out.println(correlationId);
        orderProducer.sendEvent(orderEvent);
        return orderEvent;
    }
    @Transactional
    public void updateOrderStatus(InventoryResultEvent inventoryResultEvent) {
        OrderEntity oldOrder = orderRepository.findById(inventoryResultEvent.orderId()).orElseThrow(RuntimeException::new);
        OrderStatus newOrder = OrderStatus.valueOf(String.valueOf(inventoryResultEvent.status()));
        oldOrder.setStatus(newOrder);
        orderRepository.save(oldOrder);
        System.out.println("Order status changed to " +inventoryResultEvent.orderId() + newOrder);
    }
}
