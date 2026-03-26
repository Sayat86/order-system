package com.example.ordersystem.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(template);

        FixedBackOff backOff = new FixedBackOff(2000L, 3);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // 🔥 логирование retry
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                System.out.println("Retry #" + deliveryAttempt +
                        " for message: " + record.value())
        );

        return handler;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(
            ProducerFactory<String, Object> producerFactory
    ) {

        KafkaTemplate<String, Object> template = new KafkaTemplate<>(producerFactory);
        template.setTransactionIdPrefix("tx-");

        return template;
    }
}
