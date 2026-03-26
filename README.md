
# 🧾 Order System (Event-Driven Microservice)

## 🔥 Highlights

- Event-driven architecture with Apache Kafka
- Saga choreography with compensation
- Transactional Outbox pattern
- Idempotent consumers (at-least-once safe)
- Retry & Dead Letter Queue (DLQ)
- Metrics & monitoring (Micrometer + Prometheus)

## 📌 Overview

This project is a production-style event-driven Order System built with Spring Boot, Kafka, and PostgreSQL.

It demonstrates how to build reliable distributed systems using:

* Transactional Outbox Pattern
* Saga (choreography-based)
* Idempotent consumers
* Retry & DLQ
* Observability (metrics)

---

## 🏗 Architecture

### High-level flow

```
Client → Order API → Database (Order + Outbox)
                          ↓
                   Outbox Scheduler
                          ↓
                        Kafka
                          ↓
        Payment → Inventory → Compensation (Refund)
                          ↓
                     Order Update
```

---

## ⚙️ Tech Stack

* Java 21
* Spring Boot
* Spring Data JPA
* PostgreSQL
* Apache Kafka
* Docker
* Flyway
* Micrometer + Prometheus

---

## 📂 Project Structure

```
src/main/java/com/example/ordersystem
│
├── features
│   ├── order
│   └── idempotency
│
├── messaging
│   ├── producer
│   ├── consumer
│   ├── events
│   └── model
│
├── outbox
│   ├── entity
│   ├── repository
│   ├── service
│   └── scheduler
│
├── saga
│   ├── entity
│   └── repository
│
└── config
```

---

## 🔄 Core Concepts

### 1. Transactional Outbox

Problem: DB and Kafka are separate systems → risk of inconsistency.

Solution:

* Save event to outbox_events table
* Scheduler publishes events to Kafka
* DB transaction guarantees atomicity

```
DB write + event persistence = atomic
Kafka publish = async (via scheduler)
```

---

### 2. Delivery Semantics

This system uses:

At-least-once delivery + idempotent consumers

Implemented with:

* EventEnvelope (eventId)
* processed_events table
* Idempotency checks in consumers

Guarantees:

* No lost events
* Safe reprocessing
* No duplicate side effects

---

### 3. Saga (Choreography)

Flow:

```
OrderCreated
   ↓
PaymentCompleted
   ↓
InventoryReserved → Order COMPLETED
        ↓
InventoryFailed → Refund → Order FAILED
```

Each service reacts to events independently (no central orchestrator)

---

### 4. Idempotency

Consumer level
* Each event has eventId
* Stored in processed_events
* Duplicate events are ignored

same event → processed once

API level (Order creation)
* Supports Idempotency-Key
* Same request → same response
* Prevents duplicate orders

---

### 5. Retry & Dead Letter Queue

* Automatic retries via Kafka
* Configurable backoff
* Failed messages → .DLT topics

Example:

payment-completed → payment-completed.DLT

---

### 6. Metrics & Monitoring

Exposed via:

/actuator/prometheus

Metrics:

* kafka.events.processed
* kafka.events.failed
* kafka.events.duration

Stack:

* Micrometer
* Prometheus
* Grafana

---

## 📊 Data Flow Example

```
POST /orders
   ↓
OrderService
   ↓
Save Order + OutboxEvent
   ↓
OutboxScheduler
   ↓
Kafka (order-created)
   ↓
PaymentConsumer
   ↓
InventoryConsumer
   ↓
InventoryResultConsumer
   ↓
Order Status Updated
```

---

## 🛡 Reliability Features

* Idempotent consumers
* Retry mechanism
* DLQ
* Saga compensation
* Kafka message keys (ordering per orderId)

---

## 🚀 How to Run

### 1. Start infrastructure

```
docker compose up -d
```

### 2. Run application

```
./mvnw spring-boot:run
```

### 3. Open Kafka UI

```
http://localhost:8081
```

### 4. Prometheus

```
http://localhost:9090
```

### 5. Grafana

```
http://localhost:3000
```
---

## 🧪 Example Request

```
POST /api/orders
Idempotency-Key: abc123

{
  "userId": "uuid",
  "totalAmount": 100.00
}
```

---

## 🎯 Key Problems Solved

### Dual Write Problem

Solved via Transactional Outbox.

### Distributed Transactions

Solved via Saga pattern.

### Duplicate Messages

Solved via idempotency + processed_events.

### System Failures

Handled with retry + DLQ.

---

## 📈 Future Improvements

* OpenTelemetry (tracing)
* Schema versioning for events
* Separate microservices (Order / Payment / Inventory)
* Kubernetes deployment

---

## 💡 What This Project Demonstrates

* Event-driven architecture
* Distributed consistency
* Fault-tolerant systems
* Real-world backend patterns

---

## 🏁 Summary

This project is a **production-style backend system** that demonstrates:

* Event-driven architecture
* Saga-based workflows
* Reliable message processing
* Observability and monitoring

---

## 👨‍💻 Author

Sayat Beisembayev
