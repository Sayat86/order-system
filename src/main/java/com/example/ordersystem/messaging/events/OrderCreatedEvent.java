package com.example.ordersystem.messaging.events;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
public class OrderCreatedEvent {

    private UUID orderId;

    private UUID userId;

    private BigDecimal totalAmount;

}
