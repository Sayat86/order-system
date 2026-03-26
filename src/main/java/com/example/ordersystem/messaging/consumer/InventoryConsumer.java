package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.messaging.events.InventoryFailedEvent;
import com.example.ordersystem.messaging.events.InventoryReservedEvent;
import com.example.ordersystem.messaging.events.PaymentCompletedEvent;
import com.example.ordersystem.messaging.mapper.EventMapper;
import com.example.ordersystem.messaging.model.EventEnvelope;
import com.example.ordersystem.processed.entity.ProcessedEvent;
import com.example.ordersystem.processed.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventMapper eventMapper;
    private final ProcessedEventRepository repository;

    @KafkaListener(topics = "payment-completed", groupId = "inventory-group")
    public void handlePaymentCompleted(String message) {

        EventEnvelope<PaymentCompletedEvent> envelope =
                eventMapper.read(message, PaymentCompletedEvent.class);

        if (repository.existsById(envelope.getEventId())) {
            log.info("Event already processed: {}", envelope.getEventId());
            return;
        }

        PaymentCompletedEvent event = envelope.getPayload();
        String key = event.getOrderId().toString();

        log.info("Processing event {}", envelope.getEventType());

        boolean success = false; // временно для теста

        if (success) {

            kafkaTemplate.send(
                    "inventory-reserved",
                    key,
                    eventMapper.write(
                            EventEnvelope.builder()
                                    .eventId(UUID.randomUUID())
                                    .eventType("inventory-reserved")
                                    .createdAt(Instant.now())
                                    .payload(
                                            InventoryReservedEvent.builder()
                                                    .orderId(event.getOrderId())
                                                    .build()
                                    )
                                    .build()
                    )
            );

        } else {

            kafkaTemplate.send(
                    "inventory-failed",
                    key,
                    eventMapper.write(
                            EventEnvelope.builder()
                                    .eventId(UUID.randomUUID())
                                    .eventType("inventory-failed")
                                    .createdAt(Instant.now())
                                    .payload(
                                            InventoryFailedEvent.builder()
                                                    .orderId(event.getOrderId())
                                                    .reason("Out of stock")
                                                    .build()
                                    )
                                    .build()
                    )
            );
        }

        repository.save(
                ProcessedEvent.builder()
                        .eventId(envelope.getEventId())
                        .consumerName("inventory-service")
                        .processedAt(Instant.now())
                        .build()
        );
    }
}
