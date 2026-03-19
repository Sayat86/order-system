package com.example.ordersystem.messaging.consumer;

import com.example.ordersystem.processed.entity.ProcessedEvent;
import com.example.ordersystem.processed.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final ProcessedEventRepository processedEventRepository;

    private static final String CONSUMER_NAME = "order-service";

    @KafkaListener(topics = "order-created", groupId = "order-group")
    public void consume(String payload) {

        // ❗ генерим eventId (пока упрощенно)
        UUID eventId = UUID.randomUUID();

        boolean alreadyProcessed =
                processedEventRepository.existsById(eventId);

        if (alreadyProcessed) {
            log.info("Event already processed: {}", eventId);
            return;
        }

        log.info("Processing event: {}", payload);

        // TODO: здесь будет бизнес логика (Saga)

        processedEventRepository.save(
                ProcessedEvent.builder()
                        .eventId(eventId)
                        .consumerName(CONSUMER_NAME)
                        .processedAt(Instant.now())
                        .build()
        );
    }
}
