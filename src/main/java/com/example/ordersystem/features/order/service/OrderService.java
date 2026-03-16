package com.example.ordersystem.features.order.service;

import com.example.ordersystem.features.order.dto.CreateOrderRequest;
import com.example.ordersystem.features.order.entity.Order;
import com.example.ordersystem.features.order.entity.OrderStatus;
import com.example.ordersystem.features.order.repository.OrderRepository;
import com.example.ordersystem.messaging.events.OrderCreatedEvent;
import com.example.ordersystem.outbox.service.OutboxService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxService outboxService;

    @Transactional
    public Order createOrder(CreateOrderRequest request) {

        Order order = Order.builder()
                .userId(request.getUserId())
                .totalAmount(request.getTotalAmount())
                .status(OrderStatus.PENDING)
                .createdAt(Instant.now())
                .build();

        orderRepository.save(order);

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(order.getId())
                .userId(order.getUserId())
                .totalAmount(order.getTotalAmount())
                .build();

        outboxService.saveEvent(
                "ORDER",
                order.getId(),
                "OrderCreatedEvent",
                event
        );

        return order;
    }

}
