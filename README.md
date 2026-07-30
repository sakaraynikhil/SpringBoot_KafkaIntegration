# 📡 Spring Boot Kafka Microservices Communication

This repository contains the source code for **microservices communication using Apache Kafka**. It demonstrates reliable, idempotent event-driven communication between two Spring Boot services.

---

## 🔄 Workflow

1. **Send Service (`src`)**
   - Produces and sends events to a Kafka topic.
   - Acts as the producer microservice.

2. **Receiver Service (`src2`)**
   - Consumes messages from the Kafka topic.
   - Acts as the consumer microservice.

3. **Serialization / Deserialization Handling**
   - All serialization and deserialization errors are properly managed.
   - Ensures smooth communication between producer and consumer.

4. **Dead Letter Topic (DLT)**
   - Faulty messages are automatically published to a **DLT topic**.
   - These messages can be processed later for recovery or debugging.

5. **Error Handling**
   - Utilizes Kafka’s **default error handler** to manage consumer microservice errors.
   - Prevents system crashes and ensures resilience.

6. **Idempotent & Reliable Communication**
   - Guarantees that duplicate events are handled safely.
   - Provides a robust and reliable messaging system for microservices.

---

## ⚙️ Key Features
- Event-driven communication using Apache Kafka.
- Producer (Send Service) and Consumer (Receiver Service) microservices.
- Dead Letter Topic (DLT) support for faulty messages.
- Default error handler integration for consumer resilience.
- Idempotent message processing for reliability.

---

This project serves as a **reference implementation** for building resilient, fault-tolerant microservices with Kafka in Spring Boot.
