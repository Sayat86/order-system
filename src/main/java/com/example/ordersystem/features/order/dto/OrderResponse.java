package com.example.ordersystem.features.order.dto;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
public class OrderResponse {

    private UUID id;

    private UUID userId;

    private BigDecimal totalAmount;

    private String status;

}
