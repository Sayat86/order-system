package com.example.ordersystem.outbox.service;

import com.example.ordersystem.messaging.model.EventEnvelope;
import com.example.ordersystem.outbox.entity.OutboxEvent;
import com.example.ordersystem.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OutboxService {

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;

    public void saveEvent(
            String aggregateType,
            UUID aggregateId,
            String eventType,
            Object payload
    ) {

        try {

            EventEnvelope<Object> envelope = EventEnvelope.builder()
                    .eventId(UUID.randomUUID())
                    .eventType(eventType)
                    .payload(payload)
                    .createdAt(Instant.now())
                    .build();

            String json = objectMapper.writeValueAsString(envelope);

            OutboxEvent event = OutboxEvent.builder()
                    .aggregateType(aggregateType)
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .payload(json)
                    .createdAt(Instant.now())
                    .published(false)
                    .build();

            repository.save(event);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}