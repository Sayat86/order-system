package com.example.ordersystem.messaging.mapper;

import com.example.ordersystem.messaging.model.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class EventMapper {

    private final ObjectMapper objectMapper;

    public <T> EventEnvelope<T> read(String message, Class<T> clazz) {
        try {
            return objectMapper.readValue(
                    message,
                    objectMapper.getTypeFactory()
                            .constructParametricType(EventEnvelope.class, clazz)
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String write(Object envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
