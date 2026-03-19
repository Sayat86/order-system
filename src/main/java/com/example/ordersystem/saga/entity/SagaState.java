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
    private UUID sagaId;

    private UUID orderId;

    private String currentStep;

    private String status;

    private Instant createdAt;

    private Instant updatedAt;
}
