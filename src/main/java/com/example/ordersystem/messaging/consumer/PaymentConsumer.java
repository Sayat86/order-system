package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.messaging.events.OrderCreatedEvent;
import com.example.ordersystem.messaging.events.PaymentCompletedEvent;
import com.example.ordersystem.messaging.events.PaymentFailedEvent;
import com.example.ordersystem.messaging.model.EventEnvelope;
import com.example.ordersystem.saga.entity.SagaState;
import com.example.ordersystem.saga.repository.SagaStateRepository;
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
public class PaymentConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final SagaStateRepository sagaRepository;

    @KafkaListener(topics = "order-created", groupId = "payment-group")
    public void handleOrderCreated(String message) {

        try {

            EventEnvelope<OrderCreatedEvent> envelope =
                    objectMapper.readValue(
                            message,
                            objectMapper.getTypeFactory()
                                    .constructParametricType(EventEnvelope.class, OrderCreatedEvent.class)
                    );

            OrderCreatedEvent payload = envelope.getPayload();
            UUID orderId = payload.getOrderId();

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
                        orderId.toString(),
                        objectMapper.writeValueAsString(
                                PaymentCompletedEvent.builder()
                                        .orderId(orderId)
                                        .build()
                        )
                );

            } else {

                kafkaTemplate.send(
                        "payment-failed",
                        PaymentFailedEvent.builder()
                                .orderId(orderId)
                                .reason("Payment declined")
                                .build()
                );
            }

        } catch (Exception e) {
            log.error("Payment processing failed", e);
        }
    }
}
