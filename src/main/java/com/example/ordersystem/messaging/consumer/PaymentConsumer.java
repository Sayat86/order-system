package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.messaging.events.PaymentCompletedEvent;
import com.example.ordersystem.messaging.events.PaymentFailedEvent;
import com.example.ordersystem.messaging.model.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order-created", groupId = "payment-group")
    public void handleOrderCreated(String message) {

        try {

            EventEnvelope<?> envelope =
                    objectMapper.readValue(message, EventEnvelope.class);

            log.info("Payment processing started for event {}", envelope.getEventId());

            // ❗ имитация оплаты
            boolean success = true; // пока всегда успешно

            if (success) {

                PaymentCompletedEvent event =
                        PaymentCompletedEvent.builder()
                                .orderId(
                                        UUID.fromString(
                                                ((Map<String, Object>) envelope.getPayload()).get("orderId").toString()
                                        )
                                )
                                .build();

                kafkaTemplate.send("payment-completed", event);

            } else {

                PaymentFailedEvent event =
                        PaymentFailedEvent.builder()
                                .orderId(
                                        UUID.fromString(
                                                ((Map<String, Object>) envelope.getPayload()).get("orderId").toString()
                                        )
                                )
                                .reason("Payment declined")
                                .build();

                kafkaTemplate.send("payment-failed", event);
            }

        } catch (Exception e) {
            log.error("Payment processing failed", e);
        }
    }
}
