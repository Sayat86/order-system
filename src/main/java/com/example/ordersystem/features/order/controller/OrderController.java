package com.example.ordersystem.features.order.controller;

import com.example.ordersystem.features.idempotency.service.IdempotencyService;
import com.example.ordersystem.features.order.dto.CreateOrderRequest;
import com.example.ordersystem.features.order.dto.OrderResponse;
import com.example.ordersystem.features.order.entity.Order;
import com.example.ordersystem.features.order.mapper.OrderMapper;
import com.example.ordersystem.features.order.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final IdempotencyService idempotencyService;
    private final ObjectMapper objectMapper;

    @PostMapping
    public OrderResponse createOrder(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody CreateOrderRequest request
    ) {

        String requestHash = idempotencyService.hashRequest(request);

        // 1️⃣ проверяем кеш
        Optional<String> cached =
                idempotencyService.getCachedResponse(idempotencyKey, requestHash);

        if (cached.isPresent()) {
            try {
                return objectMapper.readValue(cached.get(), OrderResponse.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // 2️⃣ создаём заказ
        Order order = orderService.createOrder(request);

        OrderResponse response = OrderMapper.toResponse(order);

        // 3️⃣ сохраняем результат
        idempotencyService.saveResponse(
                idempotencyKey,
                requestHash,
                response
        );

        return response;
    }
}
