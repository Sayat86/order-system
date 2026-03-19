package com.example.ordersystem.outbox.scheduler;

import com.example.ordersystem.outbox.entity.OutboxEvent;
import com.example.ordersystem.outbox.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxScheduler {

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void publishEvents() {

        List<OutboxEvent> events =
                repository.findTop10ByPublishedFalseOrderByCreatedAt();

        for (OutboxEvent event : events) {

            try {

                kafkaTemplate.executeInTransaction(operations -> {

                    operations.send("order-created", event.getPayload());

                    event.setPublished(true);
                    repository.save(event);

                    return true;
                });

                log.info("Event published transactionally: {}", event.getId());

            } catch (Exception e) {
                log.error("Failed to publish event {}", event.getId(), e);
            }
        }
    }
}
