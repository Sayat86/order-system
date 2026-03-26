package com.example.ordersystem.features.inventory.service;

import java.util.UUID;

public interface InventoryService {
    boolean reserve(UUID orderId);
}
