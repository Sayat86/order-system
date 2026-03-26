package com.example.ordersystem.processed.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "processed_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProcessedEvent {

    @Id
    private UUID eventId;

    @Column(name = "consumer_name")
    private String consumerName;

    @Column(name = "processed_at")
    private Instant processedAt;
}
