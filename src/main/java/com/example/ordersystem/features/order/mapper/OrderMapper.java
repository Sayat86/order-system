package com.example.ordersystem.features.order.mapper;

import com.example.ordersystem.features.order.dto.OrderResponse;
import com.example.ordersystem.features.order.entity.Order;

public class OrderMapper {

    public static OrderResponse toResponse(Order order) {

        return OrderResponse.builder()
                .id(order.getId())
                .userId(order.getUserId())
                .totalAmount(order.getTotalAmount())
                .status(order.getStatus().name())
                .build();
    }
}
