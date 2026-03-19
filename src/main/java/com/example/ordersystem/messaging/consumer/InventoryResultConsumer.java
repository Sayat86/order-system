package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.features.order.entity.Order;
import com.example.ordersystem.features.order.entity.OrderStatus;
import com.example.ordersystem.features.order.repository.OrderRepository;
import com.example.ordersystem.messaging.events.InventoryFailedEvent;
import com.example.ordersystem.messaging.events.InventoryReservedEvent;
import com.example.ordersystem.messaging.events.RefundPaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryResultConsumer {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = "inventory-reserved", groupId = "order-group")
    public void handleSuccess(InventoryReservedEvent event) {

        log.info("Inventory reserved for order {}", event.getOrderId());

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow();

        order.setStatus(OrderStatus.COMPLETED);
        orderRepository.save(order);
    }

    @KafkaListener(topics = "inventory-failed", groupId = "order-group")
    public void handleFail(InventoryFailedEvent event) {

        log.info("Inventory failed for order {}", event.getOrderId());

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
