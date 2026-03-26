package com.example.ordersystem.common.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MetricsService {

    private final MeterRegistry registry;

    public void incrementProcessed(String topic) {
        Counter.builder("kafka.events.processed")
                .tag("topic", topic)
                .register(registry)
                .increment();
    }

    public void incrementFailed(String topic) {
        Counter.builder("kafka.events.failed")
                .tag("topic", topic)
                .register(registry)
                .increment();
    }

    public Timer.Sample startTimer() {
        return Timer.start(registry);
    }

    public void stopTimer(Timer.Sample sample, String topic) {
        sample.stop(
                Timer.builder("kafka.events.duration")
                        .tag("topic", topic)
                        .register(registry)
        );
    }
}
