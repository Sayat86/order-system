package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.messaging.model.EventEnvelope;
import com.example.ordersystem.processed.entity.ProcessedEvent;
import com.example.ordersystem.processed.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProcessedEventRepository repository;
    private final ObjectMapper objectMapper;

    private static final String CONSUMER_NAME = "order-service";

    @KafkaListener(topics = "order-created", groupId = "order-group")
    public void consume(String message) {

        try {

            EventEnvelope<?> envelope =
                    objectMapper.readValue(message, EventEnvelope.class);

            if (repository.existsById(envelope.getEventId())) {
                log.info("Event already processed: {}", envelope.getEventId());
                return;
            }

            log.info("Processing event: {}", envelope.getEventType());

            // TODO: здесь будет saga логика

            repository.save(
                    ProcessedEvent.builder()
                            .eventId(envelope.getEventId())
                            .consumerName(CONSUMER_NAME)
                            .processedAt(Instant.now())
                            .build()
            );

        } catch (Exception e) {
            log.error("Failed to process event", e);
        }
    }
}