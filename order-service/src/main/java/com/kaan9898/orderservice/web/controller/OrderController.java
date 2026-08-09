package com.kaan9898.orderservice.web.controller;

import com.kaan9898.orderservice.dto.OrderCreatedEvent;
import com.kaan9898.orderservice.dto.OrderRequest;
import com.kaan9898.orderservice.service.OrderService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/order")
public class OrderController {
    private final OrderService orderService;
    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public OrderCreatedEvent createOrder(@RequestBody OrderRequest request) {
        return orderService.createOrder(request);
    }
}
