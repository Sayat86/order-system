package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.common.metrics.MetricsService;
import com.example.ordersystem.messaging.events.OrderCreatedEvent;
import com.example.ordersystem.messaging.events.PaymentCompletedEvent;
import com.example.ordersystem.messaging.events.PaymentFailedEvent;
import com.example.ordersystem.messaging.mapper.EventMapper;
import com.example.ordersystem.messaging.model.EventEnvelope;
import com.example.ordersystem.processed.entity.ProcessedEvent;
import com.example.ordersystem.processed.repository.ProcessedEventRepository;
import com.example.ordersystem.saga.entity.SagaState;
import com.example.ordersystem.saga.repository.SagaStateRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Timer;
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
public class PaymentConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SagaStateRepository sagaRepository;
    private final MetricsService metricsService;
    private final EventMapper eventMapper;
    private final ProcessedEventRepository repository;

    @KafkaListener(topics = "order-created", groupId = "payment-group")
    public void handleOrderCreated(String message) {

        Timer.Sample sample = metricsService.startTimer();

        try {

            EventEnvelope<OrderCreatedEvent> envelope =
                    eventMapper.read(message, OrderCreatedEvent.class);

            if (repository.existsById(envelope.getEventId())) {
                log.info("Event already processed: {}", envelope.getEventId());
                return;
            }

            OrderCreatedEvent payload = envelope.getPayload();
            UUID orderId = payload.getOrderId();
            String key = orderId.toString();

            log.info("Payment processing started for order {}", orderId);

            SagaState saga = SagaState.builder()
                    .sagaId(UUID.randomUUID())
                    .orderId(orderId)
                    .currentStep("PAYMENT_COMPLETED")
                    .status("IN_PROGRESS")
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            sagaRepository.save(saga);

            boolean success = true;

            if (success) {

                kafkaTemplate.send(
                        "payment-completed",
                        key,
                        eventMapper.write(
                                EventEnvelope.builder()
                                        .eventId(UUID.randomUUID())
                                        .eventType("payment-completed")
                                        .createdAt(Instant.now())
                                        .payload(
                                                PaymentCompletedEvent.builder()
                                                        .orderId(orderId)
                                                        .build()
                                        )
                                        .build()
                        )
                );

            } else {

                kafkaTemplate.send(
                        "payment-failed",
                        key,
                        eventMapper.write(
                                EventEnvelope.builder()
                                        .eventId(UUID.randomUUID())
                                        .eventType("payment-failed")
                                        .createdAt(Instant.now())
                                        .payload(
                                                PaymentFailedEvent.builder()
                                                        .orderId(orderId)
                                                        .reason("Payment declined")
                                                        .build()
                                        )
                                        .build()
                        )
                );
            }

            repository.save(
                    ProcessedEvent.builder()
                            .eventId(envelope.getEventId())
                            .consumerName("payment-service")
                            .processedAt(Instant.now())
                            .build()
            );

            metricsService.incrementProcessed("order-created");

        } catch (Exception e) {
            metricsService.incrementFailed("order-created");
            log.error("Payment processing failed", e);
            throw new RuntimeException(e);
        } finally {
            metricsService.stopTimer(sample, "order-created");
        }
    }
}
