package com.kaan9898.orderservice.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID orderId,
        Long customerId,
        @NotNull
        String product,
        @Positive
        Integer quantity,
        LocalDateTime createdTime
) {
}
