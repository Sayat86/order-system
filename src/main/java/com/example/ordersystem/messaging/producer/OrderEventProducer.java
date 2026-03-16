package com.example.ordersystem.messaging.producer;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderEventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendOrderCreated(Object event) {
        kafkaTemplate.send("order-created", event);
    }
}
