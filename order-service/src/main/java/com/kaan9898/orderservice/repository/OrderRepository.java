package com.kaan9898.orderservice.repository;

import com.kaan9898.orderservice.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;
@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {
}
