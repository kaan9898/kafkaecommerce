package com.kaan9898.notificationservice.dto;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderCancelledEvent(
        UUID orderId,
        Long customerId,
        LocalDateTime cancelledTime
) {
}
