package com.example.ordersystem.outbox.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    @GeneratedValue
    private UUID id;

    private String aggregateType;

    private UUID aggregateId;

    private String eventType;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private Instant createdAt;

    private boolean published;

}
