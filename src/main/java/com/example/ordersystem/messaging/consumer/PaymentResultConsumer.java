package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.features.order.entity.Order;
import com.example.ordersystem.features.order.entity.OrderStatus;
import com.example.ordersystem.features.order.repository.OrderRepository;
import com.example.ordersystem.messaging.events.PaymentCompletedEvent;
import com.example.ordersystem.messaging.events.PaymentFailedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentResultConsumer {

    private final OrderRepository orderRepository;

    @KafkaListener(topics = "payment-completed", groupId = "order-group")
    public void handlePaymentCompleted(PaymentCompletedEvent event) {

        log.info("Payment completed for order {}", event.getOrderId());

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow();

        order.setStatus(OrderStatus.COMPLETED);
        orderRepository.save(order);
    }

    @KafkaListener(topics = "payment-failed", groupId = "order-group")
    public void handlePaymentFailed(PaymentFailedEvent event) {

        log.info("Payment failed for order {}", event.getOrderId());

        Order order = orderRepository.findById(event.getOrderId())
                .orElseThrow();

        order.setStatus(OrderStatus.FAILED);
        orderRepository.save(order);
    }
}
