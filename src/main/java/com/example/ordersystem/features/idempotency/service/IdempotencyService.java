package com.example.ordersystem.features.idempotency.service;

import com.example.ordersystem.features.idempotency.entity.IdempotencyKey;
import com.example.ordersystem.features.idempotency.repository.IdempotencyKeyRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final IdempotencyKeyRepository repository;
    private final ObjectMapper objectMapper;

    public Optional<String> getCachedResponse(String key, String requestHash) {

        return repository.findByIdempotencyKey(key)
                .filter(record -> record.getRequestHash().equals(requestHash))
                .map(IdempotencyKey::getResponse);
    }

    public void saveResponse(String key, String requestHash, Object response) {

        try {

            String json = objectMapper.writeValueAsString(response);

            IdempotencyKey entity = IdempotencyKey.builder()
                    .id(UUID.randomUUID())
                    .idempotencyKey(key)
                    .requestHash(requestHash)
                    .response(json)
                    .createdAt(Instant.now())
                    .build();

            repository.save(entity);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String hashRequest(Object request) {

        try {
            String json = objectMapper.writeValueAsString(request);

            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(json.getBytes());

            return new String(digest);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
