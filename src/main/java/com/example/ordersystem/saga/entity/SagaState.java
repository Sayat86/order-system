package com.example.ordersystem.saga.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "saga_state")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaState {

    @Id
    @Column(name = "saga_id")
    private UUID sagaId;

    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "current_step")
    private String currentStep;

    private String status;

    @Column(name = "created_at")
    private Instant createdAt;

    @Column(name = "updated_at")
    private Instant updatedAt;
}
