package com.kaan9898.orderservice.dto;

import com.kaan9898.orderservice.entity.OrderStatus;

import java.util.UUID;

public record InventoryResultEvent(
        UUID orderId,
        OrderStatus status
) {
}
