package com.kaan9898.orderservice.dto;

public record OrderRequest(
        Long customerId,
        String product,
        Integer quantity
)
{
}
