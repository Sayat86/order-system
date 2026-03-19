package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.messaging.events.RefundPaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class RefundConsumer {

    @KafkaListener(topics = "refund-payment", groupId = "payment-group")
    public void handleRefund(RefundPaymentEvent event) {

        log.info("Refunding payment for order {}", event.getOrderId());

        // здесь будет реальный refund
    }
}
