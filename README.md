
# 🧾 Order System (Event-Driven Microservice)

## 🔥 Highlights

- Exactly-once delivery with Kafka transactions
- Saga orchestration with compensation
- Transactional outbox pattern
- Idempotent API (Stripe-like behavior)

## 📌 Overview

This project is a **production-style event-driven Order System** built with **Spring Boot, Kafka, and PostgreSQL**.

It demonstrates how to design **reliable distributed systems** with:

* Transactional Outbox Pattern
* Saga Orchestration
* Exactly-once delivery (Kafka)
* Idempotent API
* Retry & Dead Letter Queue (DLQ)

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

* Save event in DB (outbox table)
* Scheduler publishes to Kafka
* Transaction ensures atomicity

```
DB write + Kafka publish = atomic
```

---

### 2. Exactly-Once Delivery

Implemented using:

* Kafka transactions
* Idempotent producer
* Event IDs
* Processed events table

Guarantees:

* No lost events
* No duplicate processing

---

### 3. Saga Orchestration

Flow:

```
OrderCreated
   ↓
Payment
   ↓
Inventory
   ↓
Success → Order COMPLETED
Failure → Refund → Order FAILED
```

Compensation example:

```
Inventory failed → trigger refund
```

---

### 4. Idempotency (API level)

Prevents duplicate order creation:

* Client sends `Idempotency-Key`
* Request hash is stored
* Same request → same response

Used in systems like:

* Stripe
* PayPal

---

### 5. Retry & Dead Letter Queue

* Automatic retries (Kafka)
* Failed messages → DLQ
* Prevents message loss

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

* Idempotent API
* Idempotent consumers
* Retry mechanism
* DLQ
* Saga state tracking

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
* Prometheus + Grafana (metrics)
* Separate microservices (Order / Payment / Inventory)
* Kubernetes deployment

---

## 💡 What This Project Demonstrates

* Designing event-driven systems
* Handling distributed consistency
* Building fault-tolerant services
* Applying real-world microservice patterns

---

## 🏁 Summary

This project is a **production-style backend system** that demonstrates:

* Event-driven architecture
* Exactly-once processing
* Saga orchestration with compensation
* Reliable and scalable design

---

## 👨‍💻 Author

Sayat Beisembayev
