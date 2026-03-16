package com.example.ordersystem.features.order.controller;

import com.example.ordersystem.features.order.dto.CreateOrderRequest;
import com.example.ordersystem.features.order.dto.OrderResponse;
import com.example.ordersystem.features.order.entity.Order;
import com.example.ordersystem.features.order.mapper.OrderMapper;
import com.example.ordersystem.features.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public OrderResponse createOrder(@RequestBody CreateOrderRequest request) {

        Order order = orderService.createOrder(request);

        return OrderMapper.toResponse(order);
    }

}
