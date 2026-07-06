# BankStream — Architecture & Developer Guide

## Table of Contents

1. [What Is BankStream?](#1-what-is-bankstream)
2. [Why This Architecture — The Thought Process](#2-why-this-architecture--the-thought-process)
3. [The Service Map](#3-the-service-map)
4. [Infrastructure Layer](#4-infrastructure-layer)
5. [Service Deep-Dive: transaction-service](#5-service-deep-dive-transaction-service)
    - 5.1 [Domain Objects](#51-domain-objects)
    - 5.2 [Event Objects](#52-event-objects)
    - 5.3 [Repository Layer](#53-repository-layer)
    - 5.4 [Service Layer](#54-service-layer)
    - 5.5 [Producer Layer](#55-producer-layer)
    - 5.6 [Outbox Pattern](#56-outbox-pattern)
    - 5.7 [Controller Layer](#57-controller-layer)
    - 5.8 [Kafka Producer Config](#58-kafka-producer-config)
6. [Service Deep-Dive: notification-service](#6-service-deep-dive-notification-service)
    - 6.1 [Consumer Layer](#61-consumer-layer)
    - 6.2 [DLQ Producer](#62-dlq-producer)
    - 6.3 [Idempotency Guard](#63-idempotency-guard)
    - 6.4 [Kafka Consumer Config](#64-kafka-consumer-config)
7. [Kafka Cluster Architecture](#7-kafka-cluster-architecture)
8. [Topic Design](#8-topic-design)
9. [Database Schema](#9-database-schema)
10. [Runtime Data Flows](#10-runtime-data-flows)
11. [Key Design Decisions](#12-key-design-decisions)
12. [Naming Conventions](#13-naming-conventions)
13. [Project Structure](#14-project-structure)
14. [How to Add a New Event Flow (Step-by-Step)](#15-how-to-add-a-new-event-flow-step-by-step)
15. [Build & Run Reference](#16-build--run-reference)
16. [Common Gotchas](#17-common-gotchas)

---

## 1. What Is BankStream?

BankStream is a **real-time banking event processing system** built to implement and demonstrate production-grade Kafka patterns from first principles.

The key idea: **every banking event is durable, ordered, and eventually consistent across all services** — even under partial failures (broker down, service crash, network timeout).

### What does it do?

- **Accepts** transaction requests via REST API
- **Persists** transactions to Postgres atomically with an outbox entry
- **Publishes** transaction events to Kafka reliably via the outbox poller
- **Consumes** transaction events in notification-service with at-least-once delivery
- **Retries** failed processing with exponential backoff
- **Routes** permanently failed messages to a Dead Letter Queue
- **Guards** against duplicate processing via an idempotency table

### Who is this document for?

Engineers who need to:
- Understand *why* each pattern exists and the failure it prevents
- Know *where* to put new code
- Trace data flow through the entire system
- Add new event flows without breaking existing guarantees

---

## 2. Why This Architecture — The Thought Process

### The Problem with Direct Kafka Publishing

The naive approach to Kafka integration looks like this:

```java
@Transactional
public Transaction createTransaction(...) {
    repo.save(transaction);           // DB write
    kafkaTemplate.send(topic, event); // Kafka write
}
```

This seems simple but has a fatal flaw: **two separate systems, no atomicity**.

| Failure scenario | Result |
|---|---|
| Kafka down when send() is called | DB committed, event never published — silent inconsistency |
| App crashes after DB write, before send() | Same — event lost forever |
| `@Transactional` rollback after send() | Kafka message already sent, can't unsend — phantom event |

`@Transactional` does not protect you here. `JpaTransactionManager` only covers Postgres. Kafka is outside the transaction boundary entirely.

### How BankStream Solves This

```
                    ┌─────────────────────────────────────┐
                    │  Single DB Transaction               │
                    │                                      │
  REST ──────────►  │  INSERT transactions                 │
                    │  INSERT outbox (published=false)     │
                    │                                      │
                    └──────────────┬──────────────────────┘
                                   │ both or neither
                                   ▼
                    ┌─────────────────────────────────────┐
                    │  OutboxPoller (every 1s)             │
                    │                                      │
                    │  SELECT unpublished FOR UPDATE       │
                    │  SKIP LOCKED                         │
                    │  kafkaTemplate.send().get()          │
                    │  UPDATE outbox SET published=true    │
                    │                                      │
                    └──────────────┬──────────────────────┘
                                   │
                                   ▼
                              Kafka Broker
                                   │
                                   ▼
                    ┌─────────────────────────────────────┐
                    │  notification-service consumer       │
                    │                                      │
                    │  check processed_events (idempotent) │
                    │  process with retry + backoff        │
                    │  route to DLQ after max retries      │
                    │  commit offset only on success       │
                    │                                      │
                    └─────────────────────────────────────┘
```

Each layer handles exactly one failure mode:
- **Outbox** → guarantees event enters Kafka if and only if the DB write succeeded
- **At-least-once + idempotency** → guarantees no duplicate processing even on redelivery
- **DLQ** → guarantees poison pills don't block the consumer forever

---

## 3. The Service Map

```
┌─────────────────────────────────────────────────────────────────────┐
│                         BankStream                                   │
│                                                                      │
│  ┌──────────────────────┐         ┌──────────────────────────────┐  │
│  │  transaction-service  │         │     notification-service      │  │
│  │  (port 8090)          │         │     (port 8091)               │  │
│  │                       │         │                              │  │
│  │  REST API             │         │  Kafka Consumer              │  │
│  │  Business Logic       │         │  Retry + Backoff             │  │
│  │  Outbox Poller        │         │  DLQ Producer                │  │
│  │  Transaction DB       │ ──────► │  Idempotency Guard           │  │
│  └──────────────────────┘  Kafka   └──────────────────────────────┘  │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                     Infrastructure                            │   │
│  │                                                               │   │
│  │  Kafka (2 brokers, KRaft)    Schema Registry    Kafka UI     │   │
│  │  PostgreSQL 16               (port 8081)        (port 8080)  │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Services at a Glance

| Service | Port | Produces | Consumes | DB Tables |
|---|---|---|---|---|
| transaction-service | 8090 | transaction.initiated | — | transactions, outbox |
| notification-service | 8091 | transaction.dlq | transaction.initiated | processed_events |
| fraud-detection-service | 8092 (Phase 6) | fraud.alert | transaction.initiated | fraud_alerts |

---

## 4. Infrastructure Layer

### Kafka Cluster (KRaft — No ZooKeeper)

```
kafka1 (Controller + Broker)          kafka2 (Broker Only)
┌──────────────────────────┐         ┌──────────────────────────┐
│ Node ID: 1               │         │ Node ID: 2               │
│ Roles: broker,controller │         │ Roles: broker            │
│                          │◄────────│ Registers with kafka1    │
│ Leads: P0, P2            │         │ Leads: P1                │
│ Follows: P1              │         │ Follows: P0, P2          │
│                          │         │                          │
│ Ports:                   │         │ Ports:                   │
│   9092 (internal)        │         │   9094 (internal)        │
│   29092 (host)           │         │   29094 (host)           │
│   9093 (controller/Raft) │         │                          │
└──────────────────────────┘         └──────────────────────────┘
```

### Why kafka1 Is the Sole Controller

Two-node Raft with both nodes as voters requires both to be alive to form a majority — chicken-and-egg on startup. kafka1 is the sole Raft voter. kafka2 is broker-only.

Consequence:

| Broker down | Impact |
|---|---|
| kafka2 | Cluster healthy. kafka1 serves all partitions. ISR shrinks. |
| kafka1 | Controller gone. No new leader elections. kafka2 serves existing leaders temporarily. No recovery without kafka1. |

Production fix: 3 controller nodes (majority = 2, tolerate 1 failure).

### Listener Configuration

```
Internal listener (PLAINTEXT:9092/9094):
  Used by other Docker containers (Schema Registry, Spring Boot services)
  Advertised as kafka1:9092, kafka2:9094

Host listener (PLAINTEXT_HOST:29092/29094):
  Used by apps on the host machine (your Spring Boot app running locally)
  Advertised as localhost:29092, localhost:29094

Controller listener (CONTROLLER:9093):
  Used only for Raft consensus between nodes
  Never exposed outside the cluster
```

### Schema Registry

Stores Avro schemas in the Kafka `_schemas` topic (not in its own DB).
Exposes a REST API on port 8081 for schema registration and compatibility checks.
Used in Phase 5 — not active yet for JSON phases.

### Kafka UI (Provectus)

Lightweight web UI at `http://localhost:8080`.
Shows: brokers, topics, partitions, consumer groups, lag, messages, schemas.
Configured with `DYNAMIC_CONFIG_ENABLED=true` for runtime changes.

---

## 5. Service Deep-Dive: transaction-service

### Package Structure

```
com.bankstream.transaction/
├── TransactionServiceApplication.java    ← @SpringBootApplication @EnableScheduling
├── config/
│   └── KafkaProducerConfig.java          ← Two KafkaTemplate beans
├── controller/
│   └── TransactionController.java        ← POST /api/transactions
├── domain/
│   ├── Transaction.java                  ← @Entity, transactions table
│   ├── TransactionStatus.java            ← INITIATED, COMPLETED, FAILED
│   ├── TransactionType.java              ← DEBIT, CREDIT
│   └── Outbox.java                       ← @Entity, outbox table
├── event/
│   └── TransactionInitiatedEvent.java    ← Kafka message payload (not an entity)
├── outbox/
│   └── OutboxPoller.java                 ← @Scheduled, publishes unpublished entries
├── producer/
│   └── TransactionProducer.java          ← publishes TransactionInitiatedEvent to Kafka
├── repository/
│   ├── TransactionRepository.java        ← JpaRepository<Transaction, UUID>
│   └── OutboxRepository.java             ← findUnpublishedWithLock() native query
└── service/
    └── TransactionService.java           ← @Transactional, saves tx + outbox atomically
```

---

### 5.1 Domain Objects

**Location:** `domain/`

Domain objects are JPA entities mapped to Postgres tables. They are mutable (`@Data`) because Hibernate requires setters for hydration.

```java
@Entity
@Table(name = "transactions")
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class Transaction {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "account_id", nullable = false)
    private UUID accountId;

    @Column(nullable = false, precision = 15, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    private TransactionType type;       // DEBIT | CREDIT

    @Enumerated(EnumType.STRING)
    private TransactionStatus status;   // INITIATED | COMPLETED | FAILED

    @PrePersist
    void prePersist() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
        if (status == null) status = TransactionStatus.INITIATED;
        if (currency == null) currency = "INR";
    }
}
```

**Key decisions:**
- `@GeneratedValue(strategy = GenerationType.UUID)` — Hibernate generates UUID before insert, no DB sequence needed
- `@Enumerated(EnumType.STRING)` — stores enum name as string in DB, readable without decoding
- `@PrePersist` / `@PreUpdate` — timestamps set automatically, not by caller

---

### 5.2 Event Objects

**Location:** `event/`

Event objects are the Kafka message payload — separate from the domain entity. They evolve independently.

```java
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class TransactionInitiatedEvent {
    private UUID eventId;          // idempotency key for consumers
    private UUID transactionId;
    private UUID accountId;
    private BigDecimal amount;
    private String currency;
    private String type;           // String, not enum — consumers may not have the enum
    private String description;
    private Instant occurredAt;
}
```

**Why separate from Transaction entity?**

| Concern | Transaction (entity) | TransactionInitiatedEvent (event) |
|---|---|---|
| Purpose | DB persistence | Kafka message |
| Fields | All business fields | Only what consumers need |
| Types | Enums, Instant | Strings, Instant |
| Evolution | DB schema change | Kafka schema change |

If you return the entity directly as a Kafka message, every DB schema change breaks Kafka consumers. Keep them separate.

---

### 5.3 Repository Layer

**Location:** `repository/`

```java
// Standard JPA repository — nothing special
public interface TransactionRepository extends JpaRepository<Transaction, UUID> {}

// Outbox repository — native query with FOR UPDATE SKIP LOCKED
public interface OutboxRepository extends JpaRepository<Outbox, UUID> {

    @Query(value = """
        SELECT * FROM outbox
        WHERE published = false
        ORDER BY created_at
        LIMIT 100
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<Outbox> findUnpublishedWithLock();
}
```

**Why `FOR UPDATE SKIP LOCKED`?**

When multiple instances of transaction-service run simultaneously, multiple outbox pollers run simultaneously. Without locking, both instances would read the same unpublished rows and publish them twice.

`FOR UPDATE` locks the selected rows.
`SKIP LOCKED` means: if another instance already locked a row, skip it — don't block, don't duplicate.

**Why no `@Lock` annotation?**
`@Lock(LockModeType.PESSIMISTIC_WRITE)` only works with JPQL queries, not native queries. The lock clause in the SQL itself is sufficient.

---

### 5.4 Service Layer

**Location:** `service/TransactionService.java`

```java
@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Transaction initiateTransaction(UUID accountId, BigDecimal amount,
                                           TransactionType type, String description) {
        // Step 1: save business record
        Transaction transaction = Transaction.builder()
            .accountId(accountId).amount(amount).type(type).description(description)
            .build();
        transaction = transactionRepository.save(transaction);

        // Step 2: save outbox entry — SAME transaction
        // If anything fails here, BOTH rows roll back
        // Kafka is never called from this method
        TransactionInitiatedEvent event = buildEvent(transaction);
        outboxRepository.save(Outbox.builder()
            .topic("transaction.initiated")
            .partitionKey(accountId.toString())
            .payload(objectMapper.writeValueAsString(event))
            .build());

        return transaction;
        // @Transactional commits here — both rows or neither
    }
}
```

**The critical invariant:** No Kafka call inside `@Transactional`. Kafka publish is the outbox poller's responsibility, not the service's.

---

### 5.5 Producer Layer

**Location:** `producer/TransactionProducer.java`

```java
@Slf4j
@Component
public class TransactionProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TransactionProducer(
            @Qualifier("objectKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishTransactionInitiated(TransactionInitiatedEvent event) {
        // Partition key = accountId
        // All transactions for the same account → same partition → ordered per account
        String partitionKey = event.getAccountId().toString();

        kafkaTemplate.send("transaction.initiated", partitionKey, event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish event {}: {}", event.getEventId(), ex.getMessage());
                } else {
                    log.debug("Published event {} to partition {} offset {}",
                        event.getEventId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
    }
}
```

**Note:** TransactionProducer is currently used only for direct publish (Phase 2 pattern). The outbox poller uses `stringKafkaTemplate` directly — see Section 5.6.

---

### 5.6 Outbox Pattern

**Location:** `outbox/OutboxPoller.java`

```java
@Slf4j
@Component
public class OutboxPoller {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> stringKafkaTemplate;

    public OutboxPoller(
            OutboxRepository outboxRepository,
            @Qualifier("stringKafkaTemplate") KafkaTemplate<String, String> stringKafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.stringKafkaTemplate = stringKafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)  // 1 second after last completion
    @Transactional                  // lock held for entire poll cycle
    public void poll() {
        List<Outbox> unpublished = outboxRepository.findUnpublishedWithLock();
        if (unpublished.isEmpty()) return;

        for (Outbox entry : unpublished) {
            try {
                // Synchronous send — .get() blocks until broker acks
                // If Kafka is down, this throws → caught below → retry next cycle
                stringKafkaTemplate.send(
                    entry.getTopic(),
                    entry.getPartitionKey(),
                    entry.getPayload()   // already serialized JSON string
                ).get();

                entry.setPublished(true);
                entry.setPublishedAt(Instant.now());
                entry.setLastError(null);
                outboxRepository.save(entry);

            } catch (Exception e) {
                entry.setRetryCount(entry.getRetryCount() + 1);
                entry.setLastError(e.getMessage());
                outboxRepository.save(entry);

                if (entry.getRetryCount() >= MAX_RETRY) {
                    log.error("Outbox entry {} failed {} times — needs manual intervention",
                        entry.getId(), entry.getRetryCount());
                }
            }
        }
    }
}
```

**Why `stringKafkaTemplate` and not `objectKafkaTemplate`?**

The outbox `payload` field is already a JSON string (serialized at write time and stored as JSONB in Postgres).

```
objectKafkaTemplate (JsonSerializer):
  takes Object → serializes to JSON bytes
  if input is already a String → serializes the String as JSON
  result: "\"{ \\\"eventId\\\": \\\"...\\\" }\"" — JSON string inside JSON
  consumer receives: String (not Map) → wrong type

stringKafkaTemplate (StringSerializer):
  takes String → sends bytes as-is
  result: {"eventId": "..."} — correct JSON
  consumer receives: Map → correct
```

**Why `@Transactional` on the poller?**

`FOR UPDATE SKIP LOCKED` holds the row lock only for the duration of the transaction. Without `@Transactional`, the lock releases immediately after the SELECT — before the update. Two instances could then both select and publish the same row.

---

### 5.7 Controller Layer

**Location:** `controller/TransactionController.java`

```java
@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionService transactionService;

    @PostMapping
    public ResponseEntity<Transaction> initiateTransaction(
            @RequestBody InitiateTransactionRequest request) {
        Transaction transaction = transactionService.initiateTransaction(
            request.accountId(), request.amount(),
            request.type(), request.description()
        );
        return ResponseEntity.ok(transaction);
    }

    // Java 17 record — immutable DTO, no boilerplate
    public record InitiateTransactionRequest(
        UUID accountId, BigDecimal amount,
        TransactionType type, String description
    ) {}
}
```

**Why Java record for request DTO?**
Records are immutable by default and generate constructor, getters, `equals`, `hashCode`, `toString` automatically. Jackson deserializes them via the canonical constructor. No `@Data` or `@NoArgsConstructor` needed.

---

### 5.8 Kafka Producer Config

**Location:** `config/KafkaProducerConfig.java`

Two beans because the outbox poller and the direct producer need different serializers:

```java
@Configuration
public class KafkaProducerConfig {

    // For TransactionProducer — serializes typed Java objects to JSON
    @Bean("objectKafkaTemplate")
    public KafkaTemplate<String, Object> objectKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ACKS_CONFIG, "all");                    // wait for all ISR
        props.put(RETRIES_CONFIG, 3);
        props.put(ENABLE_IDEMPOTENCE_CONFIG, true);       // dedup retries
        props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // must be ≤5 for idempotence
        props.put(BATCH_SIZE_CONFIG, 32768);
        props.put(LINGER_MS_CONFIG, 5);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    // For OutboxPoller — sends pre-serialized JSON strings as-is
    @Bean("stringKafkaTemplate")
    public KafkaTemplate<String, String> stringKafkaTemplate() {
        Map<String, Object> props = new HashMap<>();
        props.put(ACKS_CONFIG, "all");
        props.put(RETRIES_CONFIG, 3);
        props.put(ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class); // no re-serialization
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
}
```

**Why `@Bean("name")` and `@Qualifier`?**

Java generics are erased at runtime — `KafkaTemplate<String, Object>` and `KafkaTemplate<String, String>` are both just `KafkaTemplate` to Spring. Without qualifier names, Spring throws `NoUniqueBeanDefinitionException`. `@Qualifier` forces explicit selection by name.

**Why `@Qualifier` requires manual constructor (no `@RequiredArgsConstructor`)?**

Lombok's `@RequiredArgsConstructor` generates constructor parameters without annotations. There is no way to tell Lombok to add `@Qualifier` to a specific parameter. Write the constructor manually when `@Qualifier` is needed.

---

## 6. Service Deep-Dive: notification-service

### Package Structure

```
com.bankstream.notification/
├── NotificationServiceApplication.java
├── config/
│   ├── KafkaConsumerConfig.java        ← MANUAL_IMMEDIATE, concurrency=3
│   └── KafkaProducerConfig.java        ← for DLQ publishing
├── consumer/
│   └── TransactionConsumer.java        ← @KafkaListener, retry loop, DLQ routing
├── dlq/
│   └── DlqProducer.java               ← publishes to transaction.dlq
├── domain/
│   └── ProcessedEvent.java            ← @Entity, idempotency table
├── repository/
│   └── ProcessedEventRepository.java
└── service/
    └── NotificationService.java       ← simulates notification send, throws on .99 amounts
```

---

### 6.1 Consumer Layer

**Location:** `consumer/TransactionConsumer.java`

The consumer follows a strict processing contract:

```
receive message
  ↓
check type (Map expected)
  → wrong type: DLQ + ack + return
  ↓
extract eventId
  → missing: DLQ + ack + return
  ↓
idempotency check (processed_events table)
  → already processed: ack + return (skip)
  ↓
retry loop (max 3 attempts, exponential backoff)
  → attempt 1: process → success: save to processed_events + ack + return
  → attempt 1: failure: wait 1s, attempt 2
  → attempt 2: process → success: save to processed_events + ack + return
  → attempt 2: failure: wait 2s, attempt 3
  → attempt 3: process → success: save to processed_events + ack + return
  → attempt 3: failure: DLQ + ack + return
```

**Why ack after DLQ routing?**

If we don't ack after routing to DLQ, the offset never advances. On restart, the consumer reads the same offset again, fails again, routes to DLQ again — infinite loop. The original topic must move on. The DLQ holds the message for manual inspection.

**Retry backoff calculation:**

```java
// attempt 1 → 1s  (2^0 * 1000)
// attempt 2 → 2s  (2^1 * 1000)
// attempt 3 → 4s  (2^2 * 1000)
// Total before DLQ: 7 seconds
// Well under max.poll.interval.ms (default 5 min) — consumer is not kicked
long backoffMs = (long) Math.pow(2, attempt - 1) * 1000;
Thread.sleep(backoffMs);
```

---

### 6.2 DLQ Producer

**Location:** `dlq/DlqProducer.java`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class DlqProducer {

    private static final String DLQ_TOPIC = "transaction.dlq";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendToDlq(String key, Object payload, String reason) {
        log.error("Routing to DLQ. Key: {}, Reason: {}", key, reason);

        kafkaTemplate.send(DLQ_TOPIC, key, payload)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    // CRITICAL: DLQ publish failed
                    // In production: page on-call, write to DB, never silently drop
                    log.error("CRITICAL: Failed to publish to DLQ for key {}: {}", key, ex.getMessage());
                }
            });
    }
}
```

**What if DLQ publish also fails?**

The message is lost. This is an acceptable tradeoff — DLQ failure means Kafka itself is severely degraded. In production, add an alert on DLQ publish failures and consider writing to a fallback DB table.

---

### 6.3 Idempotency Guard

**Location:** `domain/ProcessedEvent.java`, `repository/ProcessedEventRepository.java`

```java
@Entity
@Table(name = "processed_events")
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class ProcessedEvent {

    @Id
    @Column(name = "event_id")
    private UUID eventId;          // Primary key = the event's UUID

    @Column(name = "consumer_group", nullable = false)
    private String consumerGroup;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
```

**How it prevents duplicates:**

```
First delivery:
  existsById(eventId) → false
  process event
  INSERT processed_events (eventId) → succeeds
  ack offset

Second delivery (rebalance / broker restart):
  existsById(eventId) → true
  skip processing
  ack offset

Race condition (two instances process same message):
  Instance A: existsById → false (checks before B inserts)
  Instance B: existsById → false (checks before A inserts)
  Instance A: INSERT → succeeds
  Instance B: INSERT → DataIntegrityViolationException (PK violation)
  Instance B: catch DVE → log, ack, continue (safe to ignore)
```

The DB primary key constraint is the real idempotency enforcer — not the application-level check. The check is just an optimization to avoid processing work we know is already done.

---

### 6.4 Kafka Consumer Config

**Location:** `config/KafkaConsumerConfig.java`

```java
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());

        // MANUAL_IMMEDIATE: offset committed the moment acknowledgment.acknowledge() is called
        // vs AUTO: commits on timer regardless of processing state (at-most-once risk)
        // vs MANUAL: batches acks, commits at next poll
        factory.getContainerProperties().setAckMode(
            ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );

        // 3 concurrent consumer threads = matches partition count
        // More threads than partitions = idle threads
        factory.setConcurrency(3);

        return factory;
    }
}
```

**Consumer config decisions:**

| Config | Value | Reason |
|---|---|---|
| `auto-offset-reset` | `earliest` | On first run, start from beginning — never miss events |
| `enable-auto-commit` | `false` | Manual commit only — we control exactly when offset advances |
| `max-poll-records` | 10 | Small batches — reduces work lost on consumer failure |
| `concurrency` | 3 | Matches partition count — maximum parallelism |

---

## 7. Kafka Cluster Architecture

### Partition Distribution

```
topic: transaction.initiated (partitions=3, replication-factor=2)

Partition 0: Leader=kafka1, Follower=kafka2, ISR=[1,2]
Partition 1: Leader=kafka2, Follower=kafka1, ISR=[1,2]
Partition 2: Leader=kafka1, Follower=kafka2, ISR=[1,2]
```

Kafka distributes partition leadership evenly across brokers. Both brokers serve traffic — neither is passive.

### Replication Flow

```
Producer sends: key=ACC001, value=transaction event

1. hash("ACC001") % 3 = 0 → Partition 0
2. Partition 0 leader = kafka1
3. Producer connects directly to kafka1
4. kafka1 appends to Partition 0 log
5. kafka2 pulls new message from kafka1 (follower replication)
6. Both in ISR → kafka1 sends ack to producer
7. Producer CompletableFuture completes
```

### Consumer Group Distribution

```
Group: notification-service (3 consumers)

Consumer thread 1 → Partition 0
Consumer thread 2 → Partition 1
Consumer thread 3 → Partition 2
```

Each partition is processed by exactly one consumer thread at a time. Ordering within a partition is preserved. `setConcurrency(3)` creates 3 threads matching 3 partitions.

### What Happens When kafka2 Goes Down

```
Before:
  Partition 1 Leader=kafka2, ISR=[1,2]

kafka2 goes down:
  kafka1 (controller) detects missed heartbeat
  kafka1 elects itself as Partition 1 leader
  ISR shrinks to [1] on all partitions

After:
  Partition 0 Leader=kafka1, ISR=[1]
  Partition 1 Leader=kafka1, ISR=[1]  ← leadership moved
  Partition 2 Leader=kafka1, ISR=[1]

kafka2 comes back:
  kafka2 replicates missed messages from kafka1
  ISR restored to [1,2]
  Partition 1 preferred leader (kafka2) NOT auto-restored
  Run manually: kafka-leader-election --election-type preferred
```

---

## 8. Topic Design

| Topic | Partitions | Replication | Key | Purpose |
|---|---|---|---|---|
| `account.created` | 3 | 2 | account_id | Account creation events |
| `transaction.initiated` | 3 | 2 | account_id | New transactions — ordered per account |
| `transaction.completed` | 3 | 2 | account_id | Completed transactions |
| `transaction.failed` | 3 | 2 | account_id | Failed transactions |
| `fraud.alert` | 3 | 2 | account_id | Fraud detection results (Phase 6) |
| `transaction.dlq` | 1 | 2 | original key | Dead letter queue — single partition for ordered inspection |

### Why account_id as Partition Key?

```
With account_id as key:
  ACC001 → always Partition 2
  ACC002 → always Partition 0
  All transactions for ACC001 are in Partition 2, in order

Without a key (round-robin):
  ACC001 tx1 → Partition 0
  ACC001 tx2 → Partition 1
  ACC001 tx3 → Partition 2
  Consumed by different threads in unpredictable order
  Fraud detection sees events out of sequence → wrong results
```

### Why DLQ Has 1 Partition?

DLQ is for manual inspection. Ordering and throughput are not concerns. Single partition = simple sequential inspection of failed messages.

---

## 9. Database Schema

```sql
-- Account master data (seed data, read-only for services)
CREATE TABLE accounts (
    id             UUID PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    owner_name     VARCHAR(255) NOT NULL,
    account_type   VARCHAR(20) NOT NULL,   -- SAVINGS | CURRENT
    tier           VARCHAR(20) NOT NULL,   -- STANDARD | PREMIUM | ELITE
    balance        NUMERIC(15,2) NOT NULL,
    status         VARCHAR(20) NOT NULL,   -- ACTIVE | SUSPENDED | CLOSED
    created_at     TIMESTAMP NOT NULL
);

-- Business records — owned by transaction-service
CREATE TABLE transactions (
    id          UUID PRIMARY KEY,
    account_id  UUID NOT NULL REFERENCES accounts(id),
    amount      NUMERIC(15,2) NOT NULL,
    currency    VARCHAR(3) NOT NULL DEFAULT 'INR',
    type        VARCHAR(20) NOT NULL,    -- DEBIT | CREDIT
    status      VARCHAR(20) NOT NULL,   -- INITIATED | COMPLETED | FAILED
    description TEXT,
    created_at  TIMESTAMP NOT NULL,
    updated_at  TIMESTAMP NOT NULL
);

-- Outbox table — owned by transaction-service
-- Written atomically with transactions, read by OutboxPoller
CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    topic         VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255),
    payload       JSONB NOT NULL,          -- stored as JSONB, not varchar
    published     BOOLEAN NOT NULL DEFAULT false,
    published_at  TIMESTAMP,
    retry_count   INT NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TIMESTAMP NOT NULL
);

-- Partial index: only unpublished rows — stays small as table grows
CREATE INDEX idx_outbox_unpublished ON outbox(created_at)
    WHERE published = false;

-- Idempotency guard — owned by notification-service
CREATE TABLE processed_events (
    event_id       UUID PRIMARY KEY,       -- PK enforces uniqueness — no application-level check needed
    consumer_group VARCHAR(255) NOT NULL,
    processed_at   TIMESTAMP NOT NULL
);

-- Fraud records — owned by fraud-detection-service (Phase 6)
CREATE TABLE fraud_alerts (
    id          UUID PRIMARY KEY,
    account_id  UUID NOT NULL REFERENCES accounts(id),
    alert_type  VARCHAR(50) NOT NULL,
    details     JSONB NOT NULL,
    created_at  TIMESTAMP NOT NULL
);
```

### Why JSONB for outbox payload?

`JSONB` in Postgres is binary JSON — queryable, indexable, validated at insert time. Using `TEXT` or `VARCHAR` would store the JSON as a plain string with no validation or queryability.

**Hibernate gotcha:** Hibernate sends `String` as `VARCHAR` by default. Must annotate with both:

```java
@Column(nullable = false, columnDefinition = "jsonb")
@org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
private String payload;
```

Without `@JdbcTypeCode`, Postgres rejects the insert: `column "payload" is of type jsonb but expression is of type character varying`.

---

## 10. Runtime Data Flows

### Flow 1: Happy Path — Transaction Created Successfully

```
1. POST /api/transactions
   body: { accountId, amount, type, description }

2. TransactionController.initiateTransaction()
   → TransactionService.initiateTransaction()
   → @Transactional opens DB connection

3. transactionRepository.save(transaction)
   → Hibernate queues INSERT (not executed yet — batched)

4. outboxRepository.save(outboxEntry)
   → Hibernate queues INSERT to outbox (published=false)

5. @Transactional commits
   → BOTH inserts hit Postgres atomically
   → Either both succeed or neither does

6. HTTP 200 → Transaction entity returned to client

7. OutboxPoller fires (within 1 second)
   → SELECT unpublished FOR UPDATE SKIP LOCKED
   → finds the new outbox entry
   → stringKafkaTemplate.send("transaction.initiated", accountId, payload).get()
   → Kafka broker acks
   → UPDATE outbox SET published=true, published_at=now()
   → @Transactional commits

8. notification-service consumer receives message on partition (hash(accountId) % 3)
   → checks processed_events: not found
   → NotificationService.sendTransactionNotification(event)
   → success: INSERT processed_events
   → acknowledgment.acknowledge()
   → offset committed
```

### Flow 2: Kafka Down During Outbox Poll

```
1. Transaction saved to DB + outbox (published=false)
2. OutboxPoller fires → stringKafkaTemplate.send().get() → TimeoutException
3. outbox.retry_count++ outbox.last_error = "timeout"
4. Outbox entry stays published=false
5. Kafka comes back
6. OutboxPoller fires again → send succeeds
7. outbox.published=true
8. Consumer receives message (potentially after delay)
```

### Flow 3: Consumer Processing Failure → DLQ

```
1. Consumer receives message: amount=999.99
2. Idempotency check: not processed
3. Attempt 1: NotificationService.sendTransactionNotification() → throws RuntimeException
4. Wait 1s (backoff)
5. Attempt 2: throws again
6. Wait 2s (backoff)
7. Attempt 3: throws again
8. Max retries exhausted
9. DlqProducer.sendToDlq("transaction.dlq", key, payload, reason)
10. acknowledgment.acknowledge() ← MUST ack — moves offset forward
11. Consumer continues with next message
```

### Flow 4: Consumer Rebalance — No Message Loss

```
1. Consumer C handles Partition 2 (committed offset: 47)
2. Consumer C dies (heartbeat timeout after session.timeout.ms)
3. Coordinator detects → triggers rebalance
4. ALL consumption pauses
5. Partition 2 reassigned to Consumer A
6. Consumer A asks coordinator: last committed offset for Partition 2?
7. Coordinator: offset 47 (stored in __consumer_offsets — not in dead Consumer C)
8. Consumer A resumes from offset 48
9. Messages 48+ redelivered if Consumer C was mid-processing → idempotency handles duplicates
```

### Object Transformation Chain

```
HTTP Request JSON
  ↓ Jackson deserialization
InitiateTransactionRequest (record)
  ↓ TransactionService
Transaction (entity) + Outbox (entity) → Postgres
  ↓ objectMapper.writeValueAsString()
String (JSON) → outbox.payload (JSONB)
  ↓ OutboxPoller reads outbox.payload
String (pre-serialized JSON)
  ↓ stringKafkaTemplate (StringSerializer — no re-serialization)
Kafka message bytes
  ↓ JsonDeserializer (consumer side)
Map<String, Object> (LinkedHashMap)
  ↓ TransactionConsumer casts
Map<String, Object> event
  ↓ processed, acked
```

---

## 11. Key Design Decisions

### Decision 1: Outbox Pattern over Direct Publish

**Problem:** No atomic operation spans Postgres and Kafka.
**Decision:** Write to outbox table in same DB transaction. Separate poller publishes to Kafka.
**Tradeoff:** Up to 1 second latency between transaction creation and Kafka event.
**Alternative:** Debezium CDC (sub-millisecond, but adds operational complexity).

### Decision 2: Two KafkaTemplate Beans

**Problem:** Outbox stores payload as JSON string. Sending via JsonSerializer double-serializes it.
**Decision:** `objectKafkaTemplate` (JsonSerializer) for typed Java objects. `stringKafkaTemplate` (StringSerializer) for pre-serialized strings.
**Why not one:** Type erasure — both templates have raw type `KafkaTemplate` at runtime. `@Qualifier` required for disambiguation.

### Decision 3: Manual Offset Commit (MANUAL_IMMEDIATE)

**Problem:** Auto-commit fires on timer — can commit offsets for messages still being processed.
**Decision:** Commit offset only after successful processing AND DB write to processed_events.
**Tradeoff:** At-least-once delivery — message can be redelivered on failure. Idempotency guard handles duplicates.

### Decision 4: Idempotency via DB Primary Key

**Problem:** At-least-once delivery means duplicate messages are inevitable.
**Decision:** `processed_events` table with `event_id` as primary key. DB constraint is the enforcer — not application logic.
**Tradeoff:** Extra DB write per message. Acceptable for banking — correctness over throughput.

### Decision 5: kafka1 as Sole Controller

**Problem:** 2-node Raft with both as voters requires both alive simultaneously — startup deadlock.
**Decision:** kafka1 is controller + broker. kafka2 is broker only.
**Tradeoff:** kafka1 failure = control plane outage. Acceptable for dev. Production needs 3 controller nodes.

### Decision 6: `fixedDelay` over `fixedRate` for OutboxPoller

**Problem:** `fixedRate` fires every N ms regardless of previous execution time. If previous poll takes 30s (Kafka slow), next fires before previous completes — concurrent pollers, lock contention.
**Decision:** `fixedDelay=1000` — waits 1s after previous execution COMPLETES before starting next.
**Result:** One poll cycle at a time, no overlap.

---

## 12. Naming Conventions

| Location | Pattern | Example | Rationale |
|---|---|---|---|
| `domain/` | `<Entity>.java` | `Transaction.java` | JPA entity — matches table name |
| `domain/` | `<Entity>Status.java` | `TransactionStatus.java` | Enum for entity status field |
| `domain/` | `<Entity>Type.java` | `TransactionType.java` | Enum for entity type field |
| `event/` | `<Entity><Action>Event.java` | `TransactionInitiatedEvent.java` | Kafka payload — not an entity |
| `repository/` | `<Entity>Repository.java` | `TransactionRepository.java` | Spring Data convention |
| `service/` | `<Entity>Service.java` | `TransactionService.java` | Business logic orchestration |
| `producer/` | `<Entity>Producer.java` | `TransactionProducer.java` | Kafka message publisher |
| `consumer/` | `<Entity>Consumer.java` | `TransactionConsumer.java` | Kafka message listener |
| `outbox/` | `OutboxPoller.java` | — | Scheduled outbox publisher |
| `dlq/` | `DlqProducer.java` | — | Dead letter queue publisher |
| `config/` | `Kafka<Role>Config.java` | `KafkaProducerConfig.java` | Spring @Configuration |
| Bean names | `"<type>KafkaTemplate"` | `"objectKafkaTemplate"` | Disambiguates same raw type |
| Topics | `<domain>.<action>` | `transaction.initiated` | Dot-separated, past tense action |
| Consumer groups | `<service-name>` | `notification-service` | Matches Spring app name |

---

## 13. Project Structure

```
bankstream/
├── docker-compose.yml              ← all infrastructure
├── init.sql                        ← DB schema + seed data (runs on first Postgres boot)
├── pom.xml                         ← parent Maven POM (groupId: com.bankstream)
│
├── transaction-service/            ← port 8090
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/bankstream/transaction/
│       │   ├── TransactionServiceApplication.java
│       │   ├── config/KafkaProducerConfig.java
│       │   ├── controller/TransactionController.java
│       │   ├── domain/
│       │   │   ├── Transaction.java
│       │   │   ├── TransactionStatus.java
│       │   │   ├── TransactionType.java
│       │   │   └── Outbox.java
│       │   ├── event/TransactionInitiatedEvent.java
│       │   ├── outbox/OutboxPoller.java
│       │   ├── producer/TransactionProducer.java
│       │   ├── repository/
│       │   │   ├── TransactionRepository.java
│       │   │   └── OutboxRepository.java
│       │   └── service/TransactionService.java
│       └── resources/application.yml
│
├── notification-service/           ← port 8091
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/bankstream/notification/
│       │   ├── NotificationServiceApplication.java
│       │   ├── config/
│       │   │   ├── KafkaConsumerConfig.java
│       │   │   └── KafkaProducerConfig.java
│       │   ├── consumer/TransactionConsumer.java
│       │   ├── dlq/DlqProducer.java
│       │   ├── domain/ProcessedEvent.java
│       │   ├── repository/ProcessedEventRepository.java
│       │   └── service/NotificationService.java
│       └── resources/application.yml
│
└── fraud-detection-service/        ← port 8092 (Phase 6 — not yet implemented)
    └── pom.xml
```

---

## 14. How to Add a New Event Flow (Step-by-Step)

Example: Add `account.created` event when a new account is opened.

### Step 1: Create the event object

**File:** `transaction-service/event/AccountCreatedEvent.java`

```java
@Data @Builder @NoArgsConstructor @AllArgsConstructor
public class AccountCreatedEvent {
    private UUID eventId;
    private UUID accountId;
    private String accountNumber;
    private String ownerName;
    private String tier;
    private Instant occurredAt;
}
```

### Step 2: Add outbox write to the service

In whichever service creates accounts, add to the `@Transactional` method:

```java
AccountCreatedEvent event = AccountCreatedEvent.builder()
    .eventId(UUID.randomUUID())
    .accountId(account.getId())
    .accountNumber(account.getAccountNumber())
    .occurredAt(Instant.now())
    .build();

outboxRepository.save(Outbox.builder()
    .topic("account.created")
    .partitionKey(account.getId().toString())
    .payload(objectMapper.writeValueAsString(event))
    .build());
```

No other changes to transaction-service needed — the OutboxPoller picks it up automatically.

### Step 3: Add consumer in notification-service (or new service)

```java
@KafkaListener(topics = "account.created", groupId = "notification-service")
public void consumeAccountCreated(ConsumerRecord<String, Object> record,
                                   Acknowledgment acknowledgment) {
    // same pattern as TransactionConsumer
    // idempotency check → process → save processed_events → ack
}
```

### Step 4: Verify topic exists

```bash
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 \
  --describe --topic account.created
```

If not: create it (we disabled auto-create).

---

## 15. Build & Run Reference

### Prerequisites

- Docker Desktop running
- Java 17
- Maven 3.8+

### Start Infrastructure

```bash
cd bankstream
docker compose up -d
# Wait ~60 seconds
docker compose ps  # all 5 containers should show "healthy"
```

### Create Topics (if not exists)

```bash
for topic in account.created transaction.initiated transaction.completed transaction.failed fraud.alert; do
  docker exec -it bankstream-kafka1 kafka-topics \
    --bootstrap-server kafka1:9092 \
    --create --topic $topic \
    --partitions 3 --replication-factor 2
done

docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 \
  --create --topic transaction.dlq \
  --partitions 1 --replication-factor 2
```

### Run Services

```bash
# Terminal 1
mvn spring-boot:run -pl transaction-service

# Terminal 2
mvn spring-boot:run -pl notification-service
```

### Test Endpoints

```bash
# Normal transaction
curl -X POST http://localhost:8090/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"accountId":"a0000001-0000-0000-0000-000000000001","amount":1500.00,"type":"DEBIT","description":"ATM withdrawal"}'

# DLQ trigger (amount ending in .99 simulates failure)
curl -X POST http://localhost:8090/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"accountId":"a0000001-0000-0000-0000-000000000002","amount":999.99,"type":"DEBIT","description":"Will fail"}'
```

### Useful Kafka Commands

```bash
# Describe topic partition distribution
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 --describe --topic transaction.initiated

# Check consumer group lag
docker exec -it bankstream-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 --describe --group notification-service

# Trigger preferred leader election after broker recovery
docker exec -it bankstream-kafka1 kafka-leader-election \
  --bootstrap-server kafka1:9092 --election-type preferred \
  --topic transaction.initiated --partition 1

# Check outbox state
docker exec -it bankstream-postgres psql -U bankstream -d bankstream \
  -c "SELECT id, topic, published, retry_count, last_error FROM outbox ORDER BY created_at DESC LIMIT 10;"

# Check processed events
docker exec -it bankstream-postgres psql -U bankstream -d bankstream \
  -c "SELECT * FROM processed_events ORDER BY processed_at DESC LIMIT 10;"

# Wipe everything and start fresh
docker compose down -v && docker compose up -d
```

---

## 16. Common Gotchas

| # | Gotcha | Root Cause | Fix |
|---|---|---|---|
| 1 | `column "payload" is of type jsonb but expression is of type character varying` | Hibernate sends String as VARCHAR | Add `@JdbcTypeCode(SqlTypes.JSON)` to payload field |
| 2 | `Could not instantiate com.fasterxml.jackson.databind.JsonSerializer` | Wrong import — Jackson's abstract class | Import `org.springframework.kafka.support.serializer.JsonSerializer` |
| 3 | `Illegal attempt to set lock mode for a native query` | `@Lock` on native query | Remove `@Lock` — `FOR UPDATE SKIP LOCKED` in SQL is enough |
| 4 | `required a bean of type KafkaTemplate that could not be found` | Two beans, type erasure, Spring can't choose | Add `@Bean("name")` + `@Qualifier("name")` |
| 5 | `constructor X is already defined` | `@RequiredArgsConstructor` + manual constructor conflict | Remove `@RequiredArgsConstructor` when writing constructor manually with `@Qualifier` |
| 6 | `UnknownHostException: kafka2` | Both brokers as Raft voters — kafka1 tries to reach kafka2 before it starts | Make kafka2 broker-only, kafka1 sole controller |
| 7 | Consumer receives `String` instead of `Map` | Outbox payload double-serialized (JSON string through JsonSerializer) | Use `stringKafkaTemplate` (StringSerializer) in OutboxPoller |
| 8 | Consumer stuck on same offset forever | Not acking after DLQ routing | Always `acknowledgment.acknowledge()` even after DLQ — original topic must move on |
| 9 | Leader imbalance after broker recovery | Kafka doesn't auto-restore preferred leader | Run `kafka-leader-election --election-type preferred` manually |
| 10 | Outbox table growing unbounded | No cleanup job | Run `DELETE FROM outbox WHERE published=true AND published_at < now() - INTERVAL '7 days'` nightly |
| 11 | `auto_create_topics_enable=false` causes NoSuchTopicException | Topic not created before producer sends | Create topics explicitly before starting services — see build reference above |
| 12 | `application.yml` kafka settings ignored | Programmatic `@Configuration` bean overrides auto-config entirely | Set ALL settings in the bean — nothing is inherited from yml |
| 13 | `DataIntegrityViolationException` on processed_events insert | Race condition — two instances processed same message | Catch `DVE`, log, ack and continue — this is expected and safe |
| 14 | `version` attribute obsolete warning in docker compose | Old `version: '3.8'` top-level key | Remove the `version:` line — Docker Compose v2 doesn't need it |