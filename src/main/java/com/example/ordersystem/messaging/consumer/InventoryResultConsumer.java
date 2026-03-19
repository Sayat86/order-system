package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.features.order.entity.Order;
import com.example.ordersystem.features.order.entity.OrderStatus;
import com.example.ordersystem.features.order.repository.OrderRepository;
import com.example.ordersystem.messaging.events.InventoryFailedEvent;
import com.example.ordersystem.messaging.events.InventoryReservedEvent;
import com.example.ordersystem.messaging.events.RefundPaymentEvent;
import com.example.ordersystem.saga.entity.SagaState;
import com.example.ordersystem.saga.repository.SagaStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryResultConsumer {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SagaStateRepository sagaRepository;

    @Transactional
    @KafkaListener(topics = "inventory-reserved", groupId = "order-group")
    public void handleSuccess(InventoryReservedEvent event) {

        log.info("Inventory reserved for order {}", event.getOrderId());

        SagaState saga = sagaRepository.findByOrderId(event.getOrderId())
                .orElseThrow();

        saga.setCurrentStep("COMPLETED");
        saga.setStatus("SUCCESS");
        saga.setUpdatedAt(Instant.now());

        sagaRepository.save(saga);

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow();

        order.setStatus(OrderStatus.COMPLETED);
        orderRepository.save(order);
    }

    @Transactional
    @KafkaListener(topics = "inventory-failed", groupId = "order-group")
    public void handleFail(InventoryFailedEvent event) {

        log.info("Inventory failed for order {}", event.getOrderId());

        SagaState saga = sagaRepository.findByOrderId(event.getOrderId())
                .orElseThrow(() -> new RuntimeException("Saga not found"));

        saga.setCurrentStep("REFUND");
        saga.setStatus("FAILED");
        saga.setUpdatedAt(Instant.now());

        sagaRepository.save(saga);

        // 🔥 compensation
        kafkaTemplate.send(
                "refund-payment",
                RefundPaymentEvent.builder()
                        .orderId(event.getOrderId())
                        .build()
        );

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow();

        order.setStatus(OrderStatus.FAILED);
        orderRepository.save(order);
    }
}
