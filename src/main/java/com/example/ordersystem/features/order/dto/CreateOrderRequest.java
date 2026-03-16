package com.example.ordersystem.features.order.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Data
public class CreateOrderRequest {

    private UUID userId;

    private BigDecimal totalAmount;

}
