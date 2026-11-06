package com.kaan9898.orderservice.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderEvent(
        OrderEventType eventType,
        UUID orderId,
        Long customerId,
        String product,
        Integer quantity,
        LocalDateTime eventTime,
        String correlationId

) {
}
