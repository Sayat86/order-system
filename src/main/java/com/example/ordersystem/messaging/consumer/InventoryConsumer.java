package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.messaging.events.InventoryFailedEvent;
import com.example.ordersystem.messaging.events.InventoryReservedEvent;
import com.example.ordersystem.messaging.events.PaymentCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryConsumer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "payment-completed", groupId = "inventory-group")
    public void handlePaymentCompleted(PaymentCompletedEvent event) {

        log.info("Reserving inventory for order {}", event.getOrderId());

        boolean success = false; // ❗ делаем fail для демонстрации

        if (success) {

            kafkaTemplate.send(
                    "inventory-reserved",
                    InventoryReservedEvent.builder()
                            .orderId(event.getOrderId())
                            .build()
            );

        } else {

            kafkaTemplate.send(
                    "inventory-failed",
                    InventoryFailedEvent.builder()
                            .orderId(event.getOrderId())
                            .reason("Out of stock")
                            .build()
            );
        }
    }
}
