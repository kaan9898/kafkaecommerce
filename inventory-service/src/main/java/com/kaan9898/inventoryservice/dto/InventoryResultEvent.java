package com.kaan9898.inventoryservice.dto;

import com.kaan9898.inventoryservice.entity.InventoryStatus;

import java.util.UUID;

public record InventoryResultEvent(
        UUID orderId,
        InventoryStatus status
) {
}
