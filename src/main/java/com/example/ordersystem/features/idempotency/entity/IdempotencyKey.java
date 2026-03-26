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

    @Column(name = "idempotency_key", unique = true, nullable = false)
    private String idempotencyKey;

    @Column(name = "request_hash")
    private String requestHash;

    @Column(name = "response_payload", columnDefinition = "jsonb")
    private String response;

    @Column(name = "created_at")
    private Instant createdAt;
}
