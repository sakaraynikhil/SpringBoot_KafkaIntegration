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



# Product Event System — Kafka-Based Microservices

A two-service, event-driven system built with **Spring Boot** and **Apache Kafka**, demonstrating a production-grade producer/consumer setup with JSON serialization, error-handling deserialization, retry with backoff, and a **Dead Letter Topic (DLT)** for poison-pill message handling.

```
┌───────────────────┐        product-topic        ┌─────────────────────┐
│   senderService    │ ───────────────────────────▶ │   receiverService    │
│  (Producer + REST) │        (3 partitions)        │  (Consumer)          │
└─────────┬──────────┘                              └─────────┬────────────┘
          │                                                    │
          ▼                                                    ▼
     MySQL (product)                                DefaultErrorHandler
                                                       │  retries (2x, 1s backoff)
                                                       ▼ on exhaustion
                                              product-topic.DLT (3 partitions)
```

---

## 1. Overview

| Service | Responsibility |
|---|---|
| **senderService** | Exposes a REST API to accept `Product` payloads, persists them to MySQL, then publishes a `ProductEvent` to the `product-topic` Kafka topic. |
| **receiverService** | Subscribes to `product-topic`, deserializes incoming events, and processes them. Failed messages are retried and, if still failing, routed to a Dead Letter Topic instead of blocking the partition. |

**Tech stack:** Java 17, Spring Boot, Spring Kafka, Spring Data JPA, MySQL, Jackson JSON (de)serializer, Lombok, Log4j2.

---

## 2. senderService

### 2.1 Project structure

```
senderService/
 ├── Controller/ProductController.java
 ├── Service/ProducerService.java (interface)
 ├── Service/ServiceImpl.java
 ├── Repository/ProductRepository.java
 ├── Entity/Product.java
 ├── Event/ProductEvent.java
 ├── Config/KafkaConfig.java
 └── resources/application.properties
```

### 2.2 REST API

**`POST /api/product/save`**

Accepts a `Product` JSON body, saves it to the database, and emits a `ProductEvent` onto Kafka.

```json
{
  "id": 1,
  "product_name": "Wireless Mouse",
  "quantity": 25
}
```

Response:
- `200 OK` — `"Successfully the message has been sent with product Details saved in Database...."`
- `500 INTERNAL SERVER ERROR` — on any exception during the save/publish flow.

### 2.3 Flow of a request

1. `ProductController` receives the HTTP request and delegates to `ProducerService`.
2. `ServiceImpl.saveProduct()`:
   - Persists the incoming `Product` entity via `ProductRepository` (Spring Data JPA → MySQL).
   - Maps the persisted `Product` to a lightweight `ProductEvent` (id + product name only — the DB entity is intentionally *not* sent over the wire as-is, keeping the Kafka contract decoupled from the JPA entity).
   - Publishes the event to `product-topic` using `KafkaTemplate<String, ProductEvent>`, keyed by a randomly generated `UUID`.
   - Calls `.get()` on the returned `SendResult` future, which makes the publish **synchronous** — the calling thread blocks until the broker acknowledges the write (or throws on failure).

```java
SendResult<String, ProductEvent> result =
        kafkaTemplate.send("product-topic", productId, productEvent).get();
```

### 2.4 Kafka topic configuration (`KafkaConfig`)

```java
@Bean
public NewTopic topic(){
    return TopicBuilder.name("product-topic")
            .partitions(3)
            .replicas(1)
            .configs(Map.of("min.insync.replicas", "1"))
            .build();
}
```

- **3 partitions** allow parallel consumption on the receiver side (scaling out consumer instances within the same consumer group).
- `min.insync.replicas=1` combined with `replicas(1)` reflects a **single-broker local/dev setup**. In a multi-broker production cluster you'd raise both the replication factor and `min.insync.replicas` for durability.

### 2.5 Producer reliability settings (`application.properties`)

```properties
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JacksonJsonSerializer

spring.kafka.producer.properties.enable.idempotency=true
spring.kafka.producer.acks=all
spring.kafka.producer.retries=10
spring.kafka.producer.properties.retry.backoff.ms=1000
```

| Setting | Why it matters |
|---|---|
| `JacksonJsonSerializer` | Serializes `ProductEvent` to JSON on the wire — human-readable payloads, easy to inspect via `kafka-console-consumer` or a UI tool. |
| `enable.idempotency=true` | Prevents duplicate messages on the broker when the producer internally retries a send (e.g., after a transient network blip), by attaching sequence numbers per producer session. |
| `acks=all` | **Required** when idempotency is enabled. The producer waits for acknowledgment from all in-sync replicas before considering the write successful — the strongest durability guarantee Kafka offers. |
| `retries=10` / `retry.backoff.ms=1000` | The producer will retry a failed send up to 10 times, waiting 1 second between attempts, before giving up. |

### 2.6 Database

`Product` is a straightforward JPA entity (`id`, `product_name`, `quantity`) mapped to a `product` table via `ProductRepository extends JpaRepository<Product, Integer>`. MySQL connection details live in `application.properties`.

---

## 3. receiverService

### 3.1 Project structure

```
receiverService/
 ├── EventHandler/ProductEventHandler.java
 ├── EventModel/ProductEvent.java
 ├── Config/ConsumerConfig.java
 └── resources/application.properties
```

### 3.2 Consuming the event

```java
@Component
@KafkaListener(topics = "product-topic")
@Slf4j
public class ProductEventHandler {

    @KafkaHandler
    public void handleSaveEvent(ProductEvent productEvent){
        log.info("An event has been consumed: " + productEvent.getProduct_name());
    }
}
```

- `@KafkaListener` subscribes the component to `product-topic`.
- `@KafkaHandler` marks the method that handles incoming, successfully-deserialized payloads.
- The consumer group is `product-events-receiver` (set in `application.properties`), so scaling this service to multiple instances will automatically load-balance partitions across them.

> **Note:** in the current codebase this handler only logs the received event. If your intent is for the receiver to persist the event to its own database (a common pattern for keeping services' data stores in sync), you'd add a `Repository`/`Entity` layer here — mirroring the sender's structure — and call `repository.save(...)` inside `handleSaveEvent`. Worth calling out explicitly in the docs so the README doesn't overstate what the code currently does versus what's planned.

### 3.3 Deserialization strategy — `ErrorHandlingDeserializer`

This is the piece that makes the whole error-handling pipeline possible. Configured in `application.properties`:

```properties
spring.kafka.consumer.key-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer

spring.kafka.consumer.properties.spring.deserializer.key.delegate.class=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.properties.spring.deserializer.value.delegate.class=org.springframework.kafka.support.serializer.JacksonJsonDeserializer

spring.kafka.consumer.properties.spring.json.trusted.packages=*
spring.kafka.consumer.properties.spring.json.use.type.headers=false
spring.kafka.consumer.properties.spring.json.value.default.type=Receiver.receiverService.EventModel.ProductEvent
```

**Why not just use `JacksonJsonDeserializer` directly?**

Without a wrapper, if a malformed or unexpected payload lands on the topic (schema mismatch, corrupted bytes, a producer bug, etc.), Jackson throws a deserialization exception *before* Spring Kafka's listener container even gets a chance to hand control to your error-handling infrastructure. The consumer thread would crash outright, since the exception occurs during polling, not during message processing.

`ErrorHandlingDeserializer` solves this by:
1. Wrapping the real (delegate) deserializer — here, `JacksonJsonDeserializer`.
2. Catching any exception the delegate throws during deserialization.
3. Packaging the raw bytes + exception into a `DeserializationException` and letting the record flow *into* the listener container as a failed record, rather than killing the poll loop.
4. That failed record is then handed to the `DefaultErrorHandler` (see below) exactly like a processing-time exception would be — giving you **one unified error-handling path** for both deserialization failures and business-logic failures.

Additional settings:
- `spring.json.trusted.packages=*` — allow deserializing into types from any package (in production, scope this down to your actual model packages to reduce deserialization-based attack surface).
- `spring.json.use.type.headers=false` + `spring.json.value.default.type=...ProductEvent` — since the sender doesn't emit Jackson type headers, the receiver is told explicitly which class to deserialize into, rather than relying on headers set by the producer.

### 3.4 Error handling & retry — `DefaultErrorHandler`

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> template){
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
            template,
            (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition())
    );

    FixedBackOff backOff = new FixedBackOff(1000L, 2);
    return new DefaultErrorHandler(recoverer, backOff);
}
```

This is the core error-recovery pipeline for **both** deserialization failures (surfaced via `ErrorHandlingDeserializer`) and exceptions thrown inside `handleSaveEvent()`.

**How it behaves:**

1. A record fails (deserialization error, or an exception thrown by the `@KafkaHandler` method).
2. `DefaultErrorHandler` intercepts the failure and consults its configured `BackOff` — here, a `FixedBackOff(1000L, 2)`: **retry up to 2 times, waiting 1 second between attempts.** The same record is redelivered to the listener for each retry.
3. If all retries are exhausted and the record still fails, the `DeadLetterPublishingRecoverer` takes over instead of the container giving up on the partition:
   - It republishes the failed record — original key, value, and headers — to a **new topic**: `<original-topic>.DLT` (i.e., `product-topic.DLT`), on the **same partition number** as the original record.
   - This is what lets the consumer **keep making progress** on the rest of the partition instead of getting stuck retrying the same poison-pill message forever (which would otherwise stall consumption for every message behind it).
4. The DLT itself is declared as a real Kafka topic up front:

```java
@Bean
public NewTopic topic(){
    return TopicBuilder.name("product-topic.DLT")
            .partitions(3)
            .replicas(1)
            .build();
}
```

   Matching partition count to the source topic keeps the "same partition" recovery strategy above valid.

**Why route dead letters through a `KafkaTemplate` producer, configured inside the receiver?**

The `DeadLetterPublishingRecoverer` needs a producer to actually *publish* the failed record to the DLT — so the receiver, despite being primarily a consumer, also declares a minimal `ProducerFactory` + `KafkaTemplate` purely to support this recovery path:

```java
@Bean
public ProducerFactory<String, Object> producerFactory(){
    Map<String, Object> config = new HashMap<>();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
    return new DefaultKafkaProducerFactory<>(config);
}
```

> ⚠️ **Known issue worth fixing:** the `KEY_SERIALIZER_CLASS_CONFIG` / `VALUE_SERIALIZER_CLASS_CONFIG` here are set to `StringDeserializer` and `JacksonJsonDeserializer` — deserializer classes, not serializer classes. A producer config expects a `Serializer`, so this should be `StringSerializer` and `JacksonJsonSerializer` respectively. It happens to not break local testing if the DLT path hasn't been exercised yet, but it should be corrected before relying on this in any real failure scenario.

### 3.5 What actually reaches the DLT

Because `spring.json.value.default.type` is hard-coded to `ProductEvent`, and `ErrorHandlingDeserializer` catches failures at the deserialization layer, the kinds of failures this pipeline is built to survive include:
- Malformed/corrupt JSON on the topic.
- A payload that doesn't match the expected `ProductEvent` shape.
- Any runtime exception thrown inside `handleSaveEvent` (e.g., if a DB call were added there and the DB was unreachable).

Messages on `product-topic.DLT` retain the original key/value, so they can be inspected (e.g., via a console consumer) or replayed into `product-topic` once the underlying issue is fixed.

---

## 4. End-to-end sequence

```
Client → POST /api/product/save
   └─▶ senderService: save Product to MySQL
   └─▶ senderService: publish ProductEvent → product-topic (key=UUID)
            │
            ▼
   receiverService: ErrorHandlingDeserializer deserializes payload
            │
      ┌─────┴─────┐
      │ success   │ failure
      ▼           ▼
 handleSaveEvent   DefaultErrorHandler
 (logs event)      → retry (2×, 1s backoff)
                       │
                  still failing
                       ▼
             DeadLetterPublishingRecoverer
             → publish to product-topic.DLT
```

---

## 5. Running locally

### Prerequisites
- Java 17
- Apache Kafka (broker reachable at `localhost:9092`)
- MySQL running locally with a `company` schema (used by senderService)

### Steps

1. Start Kafka (and Zookeeper/KRaft, depending on your Kafka distribution).
2. Create the `company` database in MySQL, matching the credentials in `senderService/application.properties`.
3. Build and run each service independently:
   ```bash
   cd senderService
   ./mvnw spring-boot:run
   ```
   ```bash
   cd receiverService
   ./mvnw spring-boot:run
   ```
4. Send a test request:
   ```bash
   curl -X POST http://localhost:<sender-port>/api/product/save \
     -H "Content-Type: application/json" \
     -d '{"id": 1, "product_name": "Wireless Mouse", "quantity": 25}'
   ```
   > Both services run on `server.port=0` (a random available port). Check each service's startup logs for the actual bound port, or pin a fixed port in `application.properties` for local testing.
5. Watch the `receiverService` logs for the consumed event.
6. To test the DLT path, publish a malformed message directly to `product-topic` (e.g., via `kafka-console-producer` with plain non-JSON text) and confirm it lands on `product-topic.DLT` after the retry window.

---

## 6. Design notes & possible next steps

- **Idempotent producer + DLT consumer** together give you at-least-once delivery with duplicate protection on the send side and isolation of poison-pill messages on the receive side — a solid baseline for most transactional event pipelines.
- Consider adding **consumer-side idempotency** (e.g., checking whether an event ID has already been processed) if `handleSaveEvent` is extended to write to a database, since retries can redeliver a message that was actually processed but failed to acknowledge.
- Fix the producer/deserializer class mismatch noted in §3.4.
- Scope `spring.json.trusted.packages` down from `*` to the actual event package before production use.
- Add persistence in `receiverService` if the intent is for the consumer to write to its own datastore, not just log.
