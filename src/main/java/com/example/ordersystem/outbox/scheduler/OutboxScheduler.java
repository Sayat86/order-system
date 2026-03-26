package com.example.ordersystem.outbox.scheduler;

import com.example.ordersystem.outbox.entity.OutboxEvent;
import com.example.ordersystem.outbox.repository.OutboxEventRepository;
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

    @Scheduled(fixedDelay = 5000)
    @Transactional("transactionManager")
    public void publishEvents() {

        List<OutboxEvent> events =
                repository.findTop10ByPublishedFalseOrderByCreatedAt();

        for (OutboxEvent event : events) {

            try {

                kafkaTemplate.send("order-created",
                        event.getAggregateId().toString(),
                        event.getPayload());

                event.setPublished(true);
                repository.save(event);

                log.info("Event published: {}", event.getId());

            } catch (Exception e) {
                log.error("Failed to publish event {}", event.getId(), e);
            }
        }
    }
}
