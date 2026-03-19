package com.example.ordersystem.features.idempotency.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "idempotency_keys")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IdempotencyKey {

    @Id
    private UUID id;

    @Column(unique = true, nullable = false)
    private String key;

    private String requestHash;

    @Column(columnDefinition = "jsonb")
    private String response;

    private Instant createdAt;
}
