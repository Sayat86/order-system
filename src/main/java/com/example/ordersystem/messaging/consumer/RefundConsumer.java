package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.messaging.events.RefundPaymentEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RefundConsumer {

    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "refund-payment", groupId = "payment-group")
    public void handleRefund(String message) {

        try {

            RefundPaymentEvent event =
                    objectMapper.readValue(message, RefundPaymentEvent.class);

            log.info("Refunding payment for order {}", event.getOrderId());

            // TODO: здесь будет реальный refund

        } catch (Exception e) {
            log.error("Failed to process refund-payment event", e);
        }
    }
}
