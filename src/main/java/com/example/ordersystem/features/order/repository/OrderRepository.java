package com.example.ordersystem.features.order.repository;

import com.example.ordersystem.features.order.entity.Order;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {
}
