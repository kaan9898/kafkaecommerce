package com.kaan9898.orderservice.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import jakarta.persistence.Id;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class OrderEntity {
    @Id
    @Column(name = "order_id")
    @NotNull
    private UUID orderId;
    @Column(name = "customer_id")
    private Long customerId;
    @Column(name = "product")
    private String product;
    @Column(name = "quantity")
    private Integer quantity;
    @Column(name = "status")
    private OrderStatus status;
    @Column(name = "created_at")
    private LocalDateTime createdDate;
}
