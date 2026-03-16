package com.example.ordersystem.outbox.scheduler;

import com.example.ordersystem.messaging.producer.OrderEventProducer;
import com.example.ordersystem.outbox.entity.OutboxEvent;
import com.example.ordersystem.outbox.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxScheduler {

    private final OutboxEventRepository repository;
    private final OrderEventProducer producer;

    @Scheduled(fixedDelay = 5000)
    public void publishEvents() {

        List<OutboxEvent> events =
                repository.findTop10ByPublishedFalseOrderByCreatedAt();

        for (OutboxEvent event : events) {

            log.info("Publishing event {}", event.getEventType());

            // Kafka producer
            producer.sendOrderCreated(event.getPayload());

            event.setPublished(true);
            repository.save(event);
        }
    }
}
