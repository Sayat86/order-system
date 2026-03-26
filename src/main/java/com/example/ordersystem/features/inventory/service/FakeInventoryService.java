package com.example.ordersystem.features.inventory.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
public class FakeInventoryService implements InventoryService {

    private final Random random = new Random();

    @Override
    public boolean reserve(UUID orderId) {

        // 70% успеха, 30% отказа
        boolean success = random.nextInt(10) < 7;

        log.info("Inventory reservation for order {}: {}", orderId,
                success ? "SUCCESS" : "FAILED");

        return success;
    }
}
