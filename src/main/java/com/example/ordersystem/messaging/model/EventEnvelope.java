package com.example.ordersystem.messaging.model;

import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EventEnvelope<T> {

    private UUID eventId;
    private String eventType;
    private T payload;
    private Instant createdAt;
}
