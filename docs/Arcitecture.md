# BankStream — Architecture & Developer Guide

## Table of Contents

1. [What Is BankStream?](#1-what-is-bankstream)
2. [Why This Architecture — The Thought Process](#2-why-this-architecture--the-thought-process)
3. [The Service Map](#3-the-service-map)
4. [Infrastructure Layer](#4-infrastructure-layer)
5. [Schema Registry & Avro Serialization](#5-schema-registry--avro-serialization)
6. [Service Deep-Dive: transaction-service](#6-service-deep-dive-transaction-service)
    - 6.1 [Domain Objects](#61-domain-objects)
    - 6.2 [Event Objects (Avro)](#62-event-objects-avro)
    - 6.3 [Repository Layer](#63-repository-layer)
    - 6.4 [Service Layer](#64-service-layer)
    - 6.5 [Producer Layer](#65-producer-layer)
    - 6.6 [Outbox Pattern](#66-outbox-pattern)
    - 6.7 [Controller Layer](#67-controller-layer)
    - 6.8 [Kafka Producer Config](#68-kafka-producer-config)
    - 6.9 [Account Seeder](#69-account-seeder)
7. [Service Deep-Dive: notification-service](#7-service-deep-dive-notification-service)
    - 7.1 [Consumer Layer](#71-consumer-layer)
    - 7.2 [DLQ Producer](#72-dlq-producer)
    - 7.3 [Idempotency Guard](#73-idempotency-guard)
    - 7.4 [Kafka Consumer Config](#74-kafka-consumer-config)
8. [Service Deep-Dive: fraud-detection-service (Kafka Streams)](#8-service-deep-dive-fraud-detection-service-kafka-streams)
    - 8.1 [Why Kafka Streams Instead of @KafkaListener](#81-why-kafka-streams-instead-of-kafkalistener)
    - 8.2 [The Topology, Piece by Piece](#82-the-topology-piece-by-piece)
    - 8.3 [GlobalKTable Enrichment](#83-globalktable-enrichment)
    - 8.4 [Windowed Aggregations (Velocity & Rapid Spend)](#84-windowed-aggregations-velocity--rapid-spend)
    - 8.5 [Kafka Streams Config](#85-kafka-streams-config)
9. [Kafka Cluster Architecture](#9-kafka-cluster-architecture)
10. [Topic Design](#10-topic-design)
11. [Database Schema](#11-database-schema)
12. [Runtime Data Flows](#12-runtime-data-flows)
13. [Key Design Decisions](#13-key-design-decisions)
14. [Naming Conventions](#14-naming-conventions)
15. [Project Structure](#15-project-structure)
16. [How to Add a New Event Flow (Step-by-Step)](#16-how-to-add-a-new-event-flow-step-by-step)
17. [Build & Run Reference](#17-build--run-reference)
18. [Known Issues / Things to Clean Up](#18-known-issues--things-to-clean-up)
19. [Common Gotchas](#19-common-gotchas)

---

## 1. What Is BankStream?

BankStream is a **real-time banking event processing system** built to implement and demonstrate production-grade Kafka patterns from first principles — this is explicitly a "Kafka deep dive" repo, not a product. Each phase of the repo's history bolts on one more real-world Kafka concern.

The key idea: **every banking event is durable, ordered, schema-safe, and eventually consistent across all services** — even under partial failures (broker down, service crash, network timeout, incompatible schema change).

### What does it do?

- **Accepts** transaction requests via REST API (`transaction-service`)
- **Persists** transactions to Postgres atomically with an outbox entry
- **Publishes** transaction events to Kafka as Avro, validated against Schema Registry
- **Seeds** account reference data onto `account.created` on startup
- **Consumes** transaction events in `notification-service` with at-least-once delivery, typed Avro deserialization, retry + DLQ
- **Detects fraud** in `fraud-detection-service` using Kafka Streams — stateful windowed aggregations joined against a live account reference table
- **Guards** against duplicate processing via an idempotency table

### Who is this document for?

This is written as a working reference for a Kafka-focused learning project, covering *why* each pattern exists, the failure it prevents, and where to find/extend it. Written so future-you (or anyone picking this repo up cold) can trace data flow end-to-end without re-deriving it from the code.

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

### The Problem with Untyped JSON on the Wire

Even once delivery is solved, a second problem shows up as soon as more than one service reads the same topic: **nothing enforces that producer and consumer agree on the message shape**. A field rename or type change on the producer side silently breaks every consumer at runtime, with no compile-time or even deploy-time signal. This is what Avro + Schema Registry solves — see [Section 5](#5-schema-registry--avro-serialization).

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
                    │  kafkaTemplate.send(avroBytes).get() │
                    │  UPDATE outbox SET published=true    │
                    │                                      │
                    └──────────────┬──────────────────────┘
                                   │
                                   ▼
                    Kafka Broker  ◄──── validated against Schema Registry
                                   │
                    ┌──────────────┼───────────────────────┐
                    ▼                                       ▼
     notification-service consumer            fraud-detection-service (Kafka Streams)
     check processed_events (idempotent)      windowed aggregation + GlobalKTable join
     process with retry + backoff             emits fraud.alert on suspicious patterns
     route to DLQ after max retries
     commit offset only on success
```

Each layer handles exactly one failure mode:
- **Outbox** → guarantees event enters Kafka if and only if the DB write succeeded
- **Avro + Schema Registry** → guarantees producer/consumer agree on message shape, and schema changes are checked for compatibility before they can break anyone
- **At-least-once + idempotency** → guarantees no duplicate processing even on redelivery
- **DLQ** → guarantees poison pills don't block the consumer forever
- **Kafka Streams** → guarantees stateful, exactly-once fraud analysis without a separate stream-processing framework

---

## 3. The Service Map

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                              BankStream                                        │
│                                                                                 │
│  ┌──────────────────────┐   ┌──────────────────────┐   ┌────────────────────┐ │
│  │  transaction-service  │   │  notification-service │   │ fraud-detection-   │ │
│  │  (port 8090)          │   │  (port 8091)          │   │ service (port 8092)│ │
│  │                       │   │                       │   │                    │ │
│  │  REST API             │   │  Kafka Consumer       │   │  Kafka Streams     │ │
│  │  Business Logic       │   │  Retry + Backoff      │   │  Windowed rules    │ │
│  │  Outbox Poller        │──►│  DLQ Producer         │   │  GlobalKTable join │ │
│  │  Account Seeder       │─┐ │  Idempotency Guard    │   │  RocksDB state     │ │
│  │  Transaction DB       │ │ └──────────────────────┘   └─────────┬──────────┘ │
│  └──────────────────────┘ │           ▲                            │            │
│                            └───────────┴──── account.created ──────┘            │
│                                        (consumed by fraud-detection only)       │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐   │
│  │                          Infrastructure                                 │   │
│  │                                                                         │   │
│  │  Kafka (2 brokers, KRaft)   Schema Registry (8081)   Kafka UI (8080)   │   │
│  │  PostgreSQL 16                                                         │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────────┘
```

### Services at a Glance

| Service | Port | Produces | Consumes | DB Tables |
|---|---|---|---|---|
| transaction-service | 8090 | `transaction.initiated` (Avro), `account.created` (Avro, seeded once at startup) | — | transactions, outbox |
| notification-service | 8091 | `transaction.dlq` | `transaction.initiated` (Avro) | processed_events |
| fraud-detection-service | 8092 | `fraud.alert` (Avro) | `transaction.initiated`, `account.created` (Avro) | — (stateful, but state lives in RocksDB / changelog topics, not Postgres) |

`fraud-detection-service` is fully implemented now (Kafka Streams topology, three fraud rules) — this used to be a placeholder in earlier revisions of this doc.

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
**Now fully active** — all three services (`transaction-service` producer, `notification-service` consumer, `fraud-detection-service` Kafka Streams) go through it. See [Section 5](#5-schema-registry--avro-serialization).

### Kafka UI (Provectus)

Lightweight web UI at `http://localhost:8080`.
Shows: brokers, topics, partitions, consumer groups, lag, messages, schemas.
Configured with `DYNAMIC_CONFIG_ENABLED=true` for runtime changes.
`KAFKA_CLUSTERS_0_SCHEMAREGISTRY` points it at `http://schema-registry:8081` — schema tab shows registered subjects directly.

---

## 5. Schema Registry & Avro Serialization

This is the newest layer added to the repo and the reason three of the four services now depend on `io.confluent:kafka-avro-serializer` / `kafka-streams-avro-serde`.

### The Problem Avro + Schema Registry Solves

With plain JSON (what earlier phases of this repo used), nothing stops a producer from renaming a field or changing its type. Consumers find out at runtime, via a `NullPointerException` or a `ClassCastException`, in production. There's no registry of "what shape does this topic's messages have" and no automated compatibility check.

Avro fixes this with three pieces working together:

1. **`.avsc` schema files** — the canonical definition of a message shape, written once per event type (`src/main/avro/*.avsc` in each module).
2. **`avro-maven-plugin`** — runs in the `generate-sources` Maven phase, reads every `.avsc` under `src/main/avro`, and generates typed Java classes (builders, getters, `SpecificRecord` implementations) into `target/generated-sources/avro`. These generated classes (`TransactionInitiatedEvent`, `AccountEvent`, `FraudAlertEvent`) are what the rest of the codebase imports — they are **not** hand-written POJOs.
3. **Confluent Schema Registry** — a separate service (port 8081) that stores every schema version, assigns it a numeric ID, and enforces compatibility rules (default: `BACKWARD` — a new schema must be readable by consumers using the previous schema) before allowing registration.

### The Wire Format

A message published with `KafkaAvroSerializer` is **not** raw Avro binary — it's:

```
[magic byte (0x0)] [schema ID (4 bytes, big-endian int)] [Avro binary payload]
```

- **Magic byte** — always `0x0`, a sanity marker.
- **Schema ID** — a 4-byte integer identifying which schema version (registered in Schema Registry) encoded this payload. The consumer's `KafkaAvroDeserializer` looks this ID up (with local caching) to know how to decode the rest of the bytes.
- **Payload** — the actual Avro binary encoding, which is compact (no field names on the wire — just values in schema-defined order).

This is why you cannot just `Base64.decode()` an Avro payload and expect to read it without a deserializer that knows to strip the 5-byte header first — see the outbox quirk in [Section 6.6](#66-outbox-pattern).

### Where Each Schema Lives

| Schema | Owning service (source of truth `.avsc`) | Also copied into |
|---|---|---|
| `TransactionInitiatedEvent.avsc` | `transaction-service/src/main/avro/` | `notification-service`, `fraud-detection-service` |
| `AccountEvent.avsc` | `transaction-service/src/main/avro/` | `fraud-detection-service` |
| `FraudAlertEvent.avsc` | `fraud-detection-service/src/main/avro/` | — (only producer needs it) |

**Note on the copies:** each consuming module has its own `.avsc` copy under its own `src/main/avro/`, rather than sharing a single schema module. This means the generated Java classes live in separate packages per module (all still resolving to `com.bankstream.transaction.event.avro.TransactionInitiatedEvent`, etc., because the `.avsc` `namespace` field is what controls the generated package — not the module). This works because Avro Maven codegen doesn't care which module it runs in, only what `namespace` the schema declares. **Risk:** if the copies drift (someone edits the schema in one module but forgets the other), you get a silent mismatch that Schema Registry compatibility checks won't catch (each module registers under the same subject name, so the last one to publish "wins" the registered schema, but stale local copies still exist in source). Not currently a problem because these are all internal learning-repo copies, but worth remembering if this pattern is copied into a real multi-team system — the correct fix there is a shared schema module or a schema registry client tool (e.g. `gradle-avro-plugin` fetching from the registry) rather than hand-copied `.avsc` files.

### Schema Evolution — Why `TransactionInitiatedEvent.avsc` Has a Nullable `merchantId`

```json
{
  "name": "merchantId",
  "type": ["null", "string"],
  "default": null,
  "doc": "Optional merchant ID — added in v2, backward compatible"
}
```

Adding a field with a `default` value is a **backward-compatible** change under Schema Registry's default `BACKWARD` compatibility mode: old consumers (compiled against a schema without `merchantId`) can still read new messages (the field is just ignored), and new consumers reading old messages get `default` (`null`) for the missing field. This is the standard Avro evolution pattern — always add new fields as `["null", type]` with `"default": null`, never remove or retype an existing field without a major version bump.

### `specific.avro.reader=true`

Set in `notification-service`'s consumer config and implicitly used by `SpecificAvroSerde` in `fraud-detection-service`. This tells the Avro deserializer to produce the generated typed class (`TransactionInitiatedEvent`) instead of a generic, map-like `GenericRecord`. Without it you'd get `GenericRecord` and have to call `.get("fieldName")` with no compile-time safety — exactly the problem Avro was meant to solve.

### `ErrorHandlingDeserializer`

`notification-service`'s `KafkaConsumerConfig` wraps the real `KafkaAvroDeserializer` in Spring Kafka's `ErrorHandlingDeserializer`. Without this wrapper, a deserialization failure (e.g. a message from an incompatible schema, or a producer bug) throws inside the Kafka client's poll loop and **kills the consumer thread**. With the wrapper, the exception is caught and handed to the listener as a poison-pill record — the current code path treats it as "unexpected message type" and routes it straight to DLQ instead of crashing the app.

---

## 6. Service Deep-Dive: transaction-service

### Package Structure

```
com.bankstream.transaction/
├── TransactionServiceApplication.java    ← @SpringBootApplication @EnableScheduling
├── config/
│   └── KafkaProducerConfig.java          ← Four KafkaTemplate beans (object/avro/string/bytes)
├── controller/
│   └── TransactionController.java        ← POST /api/transactions
├── domain/
│   ├── Transaction.java                  ← @Entity, transactions table
│   ├── TransactionStatus.java            ← INITIATED, COMPLETED, FAILED
│   ├── TransactionType.java              ← DEBIT, CREDIT
│   └── Outbox.java                       ← @Entity, outbox table
├── event/
│   └── (Avro-generated — see 6.2)        ← generated into target/generated-sources/avro
├── outbox/
│   └── OutboxPoller.java                 ← @Scheduled, publishes unpublished entries as raw Avro bytes
├── producer/
│   └── TransactionProducer.java          ← publishes TransactionInitiatedEvent (Avro) to Kafka directly
├── repository/
│   ├── TransactionRepository.java        ← JpaRepository<Transaction, UUID>
│   └── OutboxRepository.java             ← findUnpublishedWithLock() native query
├── seeder/
│   └── AccountSeeder.java                ← ApplicationRunner, seeds account.created on startup
└── service/
    └── TransactionService.java           ← @Transactional, saves tx + outbox, publishes Avro event
```

---

### 6.1 Domain Objects

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

### 6.2 Event Objects (Avro)

**Location:** `src/main/avro/*.avsc` (source), generated into `target/generated-sources/avro/...` at build time — there is **no hand-written `event/` POJO anymore**.

```json
// transaction-service/src/main/avro/TransactionInitiatedEvent.avsc
{
  "type": "record",
  "name": "TransactionInitiatedEvent",
  "namespace": "com.bankstream.transaction.event.avro",
  "doc": "Event published when a transaction is initiated",
  "fields": [
    {"name": "eventId",       "type": "string"},
    {"name": "transactionId", "type": "string"},
    {"name": "accountId",     "type": "string"},
    {"name": "amount",        "type": "double"},
    {"name": "currency",      "type": "string"},
    {"name": "type",          "type": "string"},
    {"name": "description",   "type": ["null", "string"], "default": null},
    {"name": "occurredAt",    "type": "long"},
    {"name": "merchantId",    "type": ["null", "string"], "default": null,
     "doc": "Optional merchant ID — added in v2, backward compatible"}
  ]
}
```

Generated class usage — builder pattern, not `@Builder` from Lombok, this is Avro-generated code:

```java
TransactionInitiatedEvent event = TransactionInitiatedEvent.newBuilder()
        .setEventId(eventId)
        .setTransactionId(transaction.getId().toString())
        .setAccountId(transaction.getAccountId().toString())
        .setAmount(transaction.getAmount().doubleValue())
        .setCurrency(transaction.getCurrency())
        .setType(transaction.getType().name())
        .setDescription(transaction.getDescription())
        .setOccurredAt(transaction.getCreatedAt().toEpochMilli())
        .setMerchantId(null)
        .build();
```

**Why still separate from the `Transaction` entity?** Same reasoning as before Avro was introduced: DB schema changes and Kafka schema changes are two different concerns with two different evolution rules (Hibernate DDL vs. Schema Registry compatibility). Keeping them separate means a DB column rename doesn't force a wire-format break, and vice versa.

**`account.created` (`AccountEvent.avsc`)** — flat reference-data event, used to seed `fraud-detection-service`'s `GlobalKTable`:

```json
{
  "type": "record",
  "name": "AccountEvent",
  "namespace": "com.bankstream.account.event.avro",
  "fields": [
    {"name": "accountId",     "type": "string"},
    {"name": "accountNumber", "type": "string"},
    {"name": "ownerName",     "type": "string"},
    {"name": "tier",          "type": "string"},
    {"name": "status",        "type": "string"}
  ]
}
```

---

### 6.3 Repository Layer

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

### 6.4 Service Layer

**Location:** `service/TransactionService.java`

```java
@Slf4j
@Service
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final OutboxRepository outboxRepository;
    private final TransactionProducer transactionProducer;
    private final KafkaAvroSerializer kafkaAvroSerializer;

    @Transactional
    public Transaction initiateTransaction(UUID accountId, BigDecimal amount,
                                           TransactionType type, String description) {
        // Step 1: save business record
        Transaction transaction = transactionRepository.save(
            Transaction.builder().accountId(accountId).amount(amount)
                .type(type).status(TransactionStatus.INITIATED)
                .description(description).build());

        String eventId = UUID.randomUUID().toString();

        // Step 2: build the Avro event once
        TransactionInitiatedEvent avroEvent = TransactionInitiatedEvent.newBuilder()
                .setEventId(eventId)
                .setTransactionId(transaction.getId().toString())
                .setAccountId(transaction.getAccountId().toString())
                .setAmount(transaction.getAmount().doubleValue())
                .setCurrency(transaction.getCurrency())
                .setType(transaction.getType().name())
                .setDescription(transaction.getDescription())
                .setOccurredAt(transaction.getCreatedAt().toEpochMilli())
                .setMerchantId(null)
                .build();

        // Step 3: serialize to raw Avro wire bytes (magic byte + schema id + payload)
        // and write them, base64-encoded, into the outbox — same DB transaction as the entity save
        byte[] avroBytes = kafkaAvroSerializer.serialize("transaction.initiated", avroEvent);
        String base64Payload = Base64.getEncoder().encodeToString(avroBytes);

        outboxRepository.save(Outbox.builder()
                .topic("transaction.initiated")
                .partitionKey(accountId.toString())
                .payload("\"" + base64Payload + "\"")
                .build());

        // Step 4: ALSO publish directly, right now, outside the outbox flow
        transactionProducer.publishTransactionInitiated(avroEvent);

        return transaction;
        // @Transactional commits here — Transaction + Outbox rows, atomically
    }
}
```

**The critical invariant that still holds:** the DB write (transaction + outbox row) is atomic — either both commit or neither does.

**⚠️ What changed vs. the original outbox design, and why it matters:** this method now publishes to `transaction.initiated` **twice** — once (eventually, ~1s later) via the `OutboxPoller`, and once immediately via `transactionProducer.publishTransactionInitiated(avroEvent)` at the end of this same method, which is a **direct, non-transactional Kafka call inside the code path**, not gated by the outbox at all. That reintroduces exactly the dual-write problem the outbox pattern exists to prevent (see [Section 18](#18-known-issues--things-to-clean-up) — this looks like leftover code from migrating the outbox payload format from JSON to Avro, not an intentional design). Every transaction currently produces two `transaction.initiated` messages with two different `eventId`s. Both `notification-service` and `fraud-detection-service` are unaffected in practice because notification-service dedupes by `eventId` (the two messages have different IDs, so idempotency won't catch it — you'll see two notifications logged) and fraud-detection-service's velocity/spend counts will be inflated (each real transaction counts as two).

---

### 6.5 Producer Layer

**Location:** `producer/TransactionProducer.java`

```java
@Slf4j
@Component
public class TransactionProducer {

    private static final String TOPIC = "transaction.initiated";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public TransactionProducer(
            @Qualifier("avroKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishTransactionInitiated(TransactionInitiatedEvent event) {
        String partitionKey = event.getAccountId();
        kafkaTemplate.send(TOPIC, partitionKey, event)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish event {} for account {}: {}",
                        event.getEventId(), event.getAccountId(), ex.getMessage());
                } else {
                    log.debug("Published event {} to partition {} offset {}",
                        event.getEventId(), result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
    }
}
```

Uses the `avroKafkaTemplate` bean (`KafkaAvroSerializer` value serializer) — this re-serializes and re-registers the schema through the normal Confluent client path (schema-ID caching, compatibility check on first use), unlike the outbox path which serializes once manually and ships raw bytes. See [6.6](#66-outbox-pattern) for why that split exists.

---

### 6.6 Outbox Pattern

**Location:** `outbox/OutboxPoller.java`

```java
@Slf4j
@Component
public class OutboxPoller {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, byte[]> bytesKafkaTemplate;

    public OutboxPoller(OutboxRepository outboxRepository,
            @Qualifier("bytesKafkaTemplate") KafkaTemplate<String, byte[]> bytesKafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.bytesKafkaTemplate = bytesKafkaTemplate;
    }

    private static final int MAX_RETRY = 5;

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void poll() {
        List<Outbox> unpublished = outboxRepository.findUnpublishedWithLock();
        if (unpublished.isEmpty()) return;

        for (Outbox entry : unpublished) {
            try {
                // payload was stored as a JSON-quoted base64 string ("\"...\"")
                // strip the leading/trailing quote, then decode to raw Avro wire bytes
                String base64 = entry.getPayload().replaceAll("^\"|\"$", "");
                byte[] avroBytes = Base64.getDecoder().decode(base64);

                bytesKafkaTemplate.send(entry.getTopic(), entry.getPartitionKey(), avroBytes).get();

                entry.setPublished(true);
                entry.setPublishedAt(Instant.now());
                outboxRepository.save(entry);

            } catch (Exception e) {
                entry.setRetryCount(entry.getRetryCount() + 1);
                entry.setLastError(e.getMessage());
                outboxRepository.save(entry);
                if (entry.getRetryCount() >= MAX_RETRY) {
                    log.error("Outbox entry {} failed {} times, needs manual intervention",
                        entry.getId(), entry.getRetryCount());
                }
            }
        }
    }
}
```

**Why `bytesKafkaTemplate` (`ByteArraySerializer`) instead of the Avro or JSON template?**

The outbox row already holds a fully-formed Avro wire payload — magic byte + schema ID + Avro binary — produced once by `KafkaAvroSerializer.serialize(...)` in `TransactionService` (see 6.4). Sending it through `KafkaAvroSerializer` again would try to re-serialize an already-serialized `byte[]`, which is wrong; sending it through `JsonSerializer` would wrap the bytes as a JSON value, corrupting the wire format entirely. `ByteArraySerializer` sends the bytes exactly as they are — the only serializer that's a no-op passthrough. This is the same principle as the old `stringKafkaTemplate` pattern from the JSON phase (see [Decision 2](#13-key-design-decisions)), just applied to raw bytes instead of a JSON string.

**Why base64-encode the bytes before storing them in the JSONB `payload` column?**

Postgres `jsonb` (and `TEXT`) columns are for textual data — raw Avro binary can contain arbitrary byte sequences including nulls, which break both JSON validation and most text encodings. Base64 makes the binary payload safe to store as a JSON string.

**Why `@Transactional` on the poller?**

`FOR UPDATE SKIP LOCKED` holds the row lock only for the duration of the transaction. Without it, the lock releases immediately after the SELECT — before the update. Two instances could then both select and publish the same row.

**⚠️ See [Section 18](#18-known-issues--things-to-clean-up)** — this poller is currently redundant with the direct publish in `TransactionService.initiateTransaction()`.

---

### 6.7 Controller Layer

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

    public record InitiateTransactionRequest(
        UUID accountId, BigDecimal amount,
        TransactionType type, String description
    ) {}
}
```

**Why Java record for request DTO?**
Records are immutable by default and generate constructor, getters, `equals`, `hashCode`, `toString` automatically. Jackson deserializes them via the canonical constructor. No `@Data` or `@NoArgsConstructor` needed.

---

### 6.8 Kafka Producer Config

**Location:** `config/KafkaProducerConfig.java`

**Four** `ProducerFactory`/`KafkaTemplate` bean pairs now exist — one per serialization strategy in use somewhere in this service:

| Bean name | Serializer | Used by | Purpose |
|---|---|---|---|
| `objectKafkaTemplate` | `JsonSerializer` | (unused currently — legacy from the JSON phase) | Typed Java object → JSON |
| `avroKafkaTemplate` | `KafkaAvroSerializer` | `TransactionProducer` | Typed Avro object → registered Avro wire bytes, schema managed automatically |
| `stringKafkaTemplate` | `StringSerializer` | (unused currently — legacy from the JSON-outbox phase) | Pre-serialized JSON string → bytes as-is |
| `bytesKafkaTemplate` | `ByteArraySerializer` | `OutboxPoller` | Pre-serialized Avro bytes → bytes as-is, no re-encoding |

```java
@Bean("objectKafkaTemplate")
public KafkaTemplate<String, Object> objectKafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());        // JsonSerializer
}

@Bean("avroKafkaTemplate")
public KafkaTemplate<String, Object> avroKafkaTemplate() {
    return new KafkaTemplate<>(avroProducerFactory());    // KafkaAvroSerializer
}

@Bean("stringKafkaTemplate")
public KafkaTemplate<String, String> stringKafkaTemplate() {
    return new KafkaTemplate<>(stringProducerFactory());  // StringSerializer
}

@Bean("bytesKafkaTemplate")
public KafkaTemplate<String, byte[]> bytesKafkaTemplate() {
    return new KafkaTemplate<>(byteProducerFactory());    // ByteArraySerializer
}
```

Plus a standalone `KafkaAvroSerializer` bean (not wrapped in a `KafkaTemplate`) — this is what `TransactionService` calls directly (`kafkaAvroSerializer.serialize(topic, avroEvent)`) to produce raw bytes for the outbox row, rather than going through a `KafkaTemplate.send()`.

**Why `@Bean("name")` and `@Qualifier`?**

Java generics are erased at runtime — all four `KafkaTemplate<String, ...>` beans collapse to raw type `KafkaTemplate` for Spring's autowiring. Without qualifier names, Spring throws `NoUniqueBeanDefinitionException`. `@Qualifier` forces explicit selection by name.

**Why does `@Qualifier` usage mean no `@RequiredArgsConstructor` on `OutboxPoller`/`TransactionProducer`?**

Lombok's `@RequiredArgsConstructor` generates constructor parameters without annotations. There is no way to tell Lombok to add `@Qualifier` to a specific parameter. Write the constructor manually when `@Qualifier` is needed.

---

### 6.9 Account Seeder

**Location:** `seeder/AccountSeeder.java`

```java
@Slf4j
@Component
public class AccountSeeder implements ApplicationRunner {

    private static final String TOPIC = "account.created";

    private static final List<AccountEvent> SEED_ACCOUNTS = List.of(
            buildAccount("a0000001-0000-0000-0000-000000000001", "ACC001", "Nishanth Kumar", "PREMIUM", "ACTIVE"),
            buildAccount("a0000001-0000-0000-0000-000000000002", "ACC002", "Ravi Shankar", "STANDARD", "ACTIVE"),
            buildAccount("a0000001-0000-0000-0000-000000000003", "ACC003", "Priya Mehta", "ELITE", "ACTIVE")
    );

    @Override
    public void run(ApplicationArguments args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        try (KafkaProducer<String, AccountEvent> producer = new KafkaProducer<>(props)) {
            for (AccountEvent account : SEED_ACCOUNTS) {
                producer.send(new ProducerRecord<>(TOPIC, account.getAccountId(), account), (metadata, ex) -> { /* log */ });
            }
            producer.flush();
        }
    }
}
```

**What this is for:** `fraud-detection-service`'s `GlobalKTable` (see [8.3](#83-globalktable-enrichment)) needs `account.created` events to know each account's tier (used for the high-value fraud threshold). This seeder writes the same three accounts that exist in `init.sql`'s `accounts` table directly onto the Kafka topic on `transaction-service` startup, using a raw `KafkaProducer` (not a Spring `KafkaTemplate` bean) configured inline.

**Why `ApplicationRunner` instead of e.g. a `CommandLineRunner` or a manual endpoint?** `ApplicationRunner` runs once, automatically, right after the Spring context is fully initialized — appropriate for "prime the topic with reference data on every service start" semantics. Since `account.created` messages are keyed by `accountId` and Kafka topics are logs (not queues), re-running the seeder on every restart just re-appends the same three account states — harmless, and `GlobalKTable` always resolves to the latest value per key.

**Why does the seeder duplicate `init.sql`'s account data instead of the app reading from Postgres?** Because this is deliberately Kafka-native reference data for the Streams app — `fraud-detection-service` has zero Postgres dependency and gets everything it needs from Kafka topics, which is the idiomatic Kafka Streams pattern (see [8.1](#81-why-kafka-streams-instead-of-kafkalistener)). Keeping the seed data in sync with `init.sql` by hand is a manual step — if you add an account to one, add it to the other.

---

## 7. Service Deep-Dive: notification-service

### Package Structure

```
com.bankstream.notification/
├── NotificationServiceApplication.java
├── config/
│   ├── KafkaConsumerConfig.java        ← ErrorHandlingDeserializer + KafkaAvroDeserializer, MANUAL_IMMEDIATE, concurrency=3
│   └── KafkaProducerConfig.java        ← plain JsonSerializer template, used only for DLQ
├── consumer/
│   └── TransactionConsumer.java        ← @KafkaListener, typed Avro object, retry loop, DLQ routing
├── dlq/
│   └── DlqProducer.java               ← publishes to transaction.dlq
├── domain/
│   └── ProcessedEvent.java            ← @Entity, idempotency table
├── repository/
│   └── ProcessedEventRepository.java
└── service/
    └── NotificationService.java       ← takes a typed TransactionInitiatedEvent, simulates failure on .99 amounts
```

---

### 7.1 Consumer Layer

**Location:** `consumer/TransactionConsumer.java`

```
receive ConsumerRecord<String, Object>
  ↓
value instanceof TransactionInitiatedEvent?
  → no (ErrorHandlingDeserializer already caught a deser failure, or wrong type):
    DLQ + ack + return
  ↓
extract eventId directly — event.getEventId() is a typed String field now, no map.get("eventId") casting
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

The biggest change from the JSON phase: with `specific.avro.reader=true` (see [Section 5](#5-schema-registry--avro-serialization)), `record.value()` deserializes straight into the generated `TransactionInitiatedEvent` class — `event.getEventId()`, `event.getAccountId()` etc. are now typed field accessors, not `Map.get("...")` casts with no compile-time safety.

**Why ack after DLQ routing?**

If we don't ack after routing to DLQ, the offset never advances. On restart, the consumer reads the same offset again, fails again, routes to DLQ again — infinite loop. The original topic must move on. The DLQ holds the message for manual inspection.

**Retry backoff calculation:**

```java
// attempt 1 → 1s  (2^0 * 1000)
// attempt 2 → 2s  (2^1 * 1000)
// attempt 3 → 4s  (2^2 * 1000)
// Total before DLQ: 7 seconds — well under max.poll.interval.ms (default 5 min)
long backoffMs = (long) Math.pow(2, attempt - 1) * 1000;
Thread.sleep(backoffMs);
```

---

### 7.2 DLQ Producer

**Location:** `dlq/DlqProducer.java`

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class DlqProducer {

    private static final String DLQ_TOPIC = "transaction.dlq";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendToDlq(String key, Object payload, String reason) {
        log.error("Routing message to DLQ. Key:{}, Reason:{}", key, reason);
        kafkaTemplate.send(DLQ_TOPIC, key, payload)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("CRITICAL: Failed to publish to DLQ for key {}: {}", key, ex.getMessage());
                }
            });
    }
}
```

Note this still uses the plain `JsonSerializer`-backed `kafkaTemplate` bean, **not** Avro — the DLQ payload can be either a raw Avro `SpecificRecord` (Jackson serializes it via its getters) or an arbitrary malformed value, so keeping the DLQ itself schema-free is deliberate: it must be able to accept anything that failed to conform, including things that aren't valid Avro at all.

**What if DLQ publish also fails?**

The message is lost. This is an acceptable tradeoff — DLQ failure means Kafka itself is severely degraded. In production, add an alert on DLQ publish failures and consider writing to a fallback DB table.

---

### 7.3 Idempotency Guard

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

**Reminder:** this guard dedupes by `eventId`, and the [dual-publish issue](#18-known-issues--things-to-clean-up) in `transaction-service` currently generates two *different* `eventId`s per logical transaction — so this guard will **not** catch that particular duplication. It only protects against Kafka-level redelivery of the *same* message (rebalances, retries, at-least-once semantics), which is the problem it was actually built for.

---

### 7.4 Kafka Consumer Config

**Location:** `config/KafkaConsumerConfig.java`

```java
@Bean
public ConsumerFactory<String, Object> consumerFactory() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

    // ErrorHandlingDeserializer wraps the real deserializer — a deser failure
    // is handed to the listener as a poison record instead of killing the consumer thread
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
    props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
    props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);

    props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

    return new DefaultKafkaConsumerFactory<>(props);
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    factory.setConcurrency(3);
    return factory;
}
```

**Consumer config decisions:**

| Config | Value | Reason |
|---|---|---|
| `auto-offset-reset` | `earliest` | On first run, start from beginning — never miss events |
| `enable-auto-commit` | `false` | Manual commit only — we control exactly when offset advances |
| `max-poll-records` | 10 | Small batches — reduces work lost on consumer failure |
| `concurrency` | 3 | Matches partition count — maximum parallelism |
| `specific.avro.reader` | `true` | Deserialize to the generated typed class, not `GenericRecord` |

---

## 8. Service Deep-Dive: fraud-detection-service (Kafka Streams)

This service is fully implemented — it used to be a Phase 6 placeholder in earlier revisions of this doc, now it's a real, non-trivial Kafka Streams topology.

### Package Structure

```
com.bankstream.fraud/
├── FraudDetectionApplication.java
├── config/
│   └── KafkaStreamsConfig.java     ← @EnableKafkaStreams, StreamsConfig props, avroSerde() factory
├── model/
│   └── AccountTier.java            ← STANDARD/PREMIUM/ELITE, per-tier high-value threshold
└── streams/
    └── FraudDetectionTopology.java ← @Autowired-injected StreamsBuilder, defines the whole DAG
```

### 8.1 Why Kafka Streams Instead of `@KafkaListener`

`notification-service` uses `@KafkaListener` — simple, stateless, one record at a time. Fraud detection needs things `@KafkaListener` cannot give you without hand-building them:

- **State across messages** — "has this account made ≥3 transactions in the last 5 minutes?" requires remembering a rolling count per key. `@KafkaListener` has no built-in state store; you'd have to build this yourself (e.g., in Redis) and handle failure/recovery of that state manually.
- **Joins against another stream/table** — "what's this account's tier?" requires correlating two topics (`transaction.initiated` and `account.created`) by key. Kafka Streams' `GlobalKTable` gives you this as a first-class operator.
- **Windowing with correctness guarantees** — sliding/tumbling windows, grace periods for late data, and exactly-once processing are all built into the DSL rather than hand-rolled.

Kafka Streams gets you all of this as a **library**, not a separate cluster — it runs embedded inside the Spring Boot app (`spring-kafka`'s `@EnableKafkaStreams` wires a `StreamsBuilderFactoryBean`), with state persisted locally in RocksDB and backed by Kafka changelog topics for fault tolerance.

### 8.2 The Topology, Piece by Piece

**Location:** `streams/FraudDetectionTopology.java`, built inside a method Spring Kafka calls automatically:

```java
@Autowired
public void buildTopology(StreamsBuilder builder) {
    var transactionSerde = streamsConfig.<TransactionInitiatedEvent>avroSerde();
    var accountSerde     = streamsConfig.<AccountEvent>avroSerde();
    var fraudAlertSerde  = streamsConfig.<FraudAlertEvent>avroSerde();

    GlobalKTable<String, AccountEvent> accountTable = builder.globalTable(
            "account.created", Consumed.with(Serdes.String(), accountSerde));

    KStream<String, TransactionInitiatedEvent> transactions = builder.stream(
            "transaction.initiated", Consumed.with(Serdes.String(), transactionSerde));

    // ... three fraud rules attached to `transactions`, see 8.3 / 8.4 ...
}
```

Spring Kafka calls any `@Autowired`-annotated method that takes a `StreamsBuilder` parameter after constructing the `StreamsBuilder` bean but before starting the streams application — this is the hook point for defining your topology. There is no `main()`-style "run the pipeline" call; the DSL calls (`.stream()`, `.groupByKey()`, `.to()`, etc.) *build a processing graph declaratively*, which `spring-kafka` then starts as a background thread pool once the Spring context finishes initializing.

Three fraud rules are attached, all writing to `fraud.alert`:

| Rule | Alert Type | Trigger | Needs account data? |
|---|---|---|---|
| Velocity | `VELOCITY` | ≥3 transactions for one account in a 5-minute sliding window | No — raw transaction stream only |
| Rapid large spend | `RAPID_LARGE` | Total spend for one account exceeds ₹1,00,000 in a 1-hour tumbling window | No — raw transaction stream only |
| High value | `HIGH_VALUE` | A single transaction exceeds the account's tier-specific threshold | Yes — joined against `GlobalKTable` |

### 8.3 GlobalKTable Enrichment

```java
GlobalKTable<String, AccountEvent> accountTable = builder.globalTable(
        "account.created", Consumed.with(Serdes.String(), accountSerde));

KStream<String, EnrichedTransaction> enriched = transactions.join(
        accountTable,
        (key, transaction) -> transaction.getAccountId(),   // foreign-key extractor
        (transaction, account) -> new EnrichedTransaction(transaction, account)
);

enriched
    .filter((accountId, et) -> {
        double threshold = AccountTier.fromString(et.account().getTier()).getHighValueThreshold();
        return et.transaction().getAmount() > threshold;
    })
    .map((accountId, et) -> KeyValue.pair(accountId, buildAlert(et.transaction(), et.account(), "HIGH_VALUE", ...)))
    .to("fraud.alert", Produced.with(Serdes.String(), fraudAlertSerde));
```

**Why `GlobalKTable` and not a regular `KTable`?**

A regular `KTable` is partitioned the same way as the stream it's built from — a join only works correctly if both sides are co-partitioned (same key, same partition count, same partitioning strategy). `account.created` and `transaction.initiated` are keyed the same way (`accountId`) here, so a `KTable` join *would* technically work — but `GlobalKTable` is used instead because:

- The full account reference dataset is small (a handful of accounts in this repo) and needed by every stream task, not just the partition-matching one.
- `GlobalKTable` is fully replicated to every Streams instance — no re-partitioning needed, no risk of a mismatched partition count between the two topics silently breaking the join.
- It's the idiomatic Kafka Streams pattern for "small, slowly-changing reference/dimension data joined against a large, high-throughput fact stream" — exactly this use case (accounts vs. transactions).

`EnrichedTransaction` is a local `record` (not Avro-generated) — it exists only inside the topology to carry both sides of the join through the `.filter()`/`.map()` chain; it never touches Kafka.

### 8.4 Windowed Aggregations (Velocity & Rapid Spend)

```java
// Velocity — sliding window, 5 minutes, grace period 30s for late-arriving events
transactions
    .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
    .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(30)))
    .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("velocity-count-store")
        .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
    .toStream()
    .filter((windowedKey, count) -> count != null && count >= 3)
    .map((windowedKey, count) -> KeyValue.pair(windowedKey.key(), buildVelocityAlert(windowedKey, count)))
    .to("fraud.alert", Produced.with(Serdes.String(), fraudAlertSerde));

// Rapid large spend — tumbling window, 1 hour
transactions
    .groupByKey(Grouped.with(Serdes.String(), transactionSerde))
    .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofSeconds(30)))
    .aggregate(() -> 0.0, (accountId, tx, total) -> total + tx.getAmount(),
        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("hourly-spend-store")
            .withKeySerde(Serdes.String()).withValueSerde(Serdes.Double()))
    .toStream()
    .filter((windowedKey, total) -> total != null && total > 1_00_000.0)
    .map((windowedKey, total) -> KeyValue.pair(windowedKey.key(), buildSpendAlert(windowedKey, total)))
    .to("fraud.alert", Produced.with(Serdes.String(), fraudAlertSerde));
```

**Sliding vs. tumbling window — why different window types for the two rules?**

- **Velocity uses a sliding window**: "3 transactions in *any* 5-minute period" needs to catch bursts that straddle a window boundary. A tumbling window (fixed, non-overlapping 5-min buckets) could miss a burst that happens 2 minutes before one bucket ends and 3 minutes into the next — the count would be split across two buckets and never hit 3 in either. Sliding windows evaluate a new window on every event, catching bursts regardless of alignment.
- **Rapid spend uses a tumbling window**: hourly spend limits are naturally aligned to fixed hour-long buckets — no need for the (more expensive) sliding window overlap here, and "total spend this hour" is a simpler, cheaper aggregate that doesn't need sub-window precision.

**Why a grace period?** Real event streams have out-of-order arrival (network delay, retries). The grace period keeps a window open for late-arriving events for an extra 30 seconds after its nominal close time before finalizing the aggregate — without it, a slightly-late event would either be dropped or land in the wrong window.

**Why `Materialized.as("store-name")`?** Naming the state store makes it queryable (via Interactive Queries, not currently used here) and gives it a durable, named RocksDB directory + a named Kafka changelog topic for fault-tolerant recovery — if the app restarts, the aggregate state is rebuilt from the changelog rather than lost.

### 8.5 Kafka Streams Config

**Location:** `config/KafkaStreamsConfig.java`

```java
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);   // also the consumer group ID
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);             // RocksDB location on disk
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);        // default 30s, lowered for faster recovery/demo visibility
        return new KafkaStreamsConfiguration(props);
    }

    public <T extends SpecificRecord> SpecificAvroSerde<T> avroSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return serde;
    }
}
```

**Why the bean must be named exactly `KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME`?** Spring Kafka's `@EnableKafkaStreams` looks up the `KafkaStreamsConfiguration` bean by this exact name to build the underlying `StreamsBuilderFactoryBean`. Any other bean name means Spring Kafka can't find your config and either fails to start or falls back to defaults.

**Why `EXACTLY_ONCE_V2`?** Without it, Kafka Streams gives at-least-once processing — a crash mid-aggregation-update could double-count a transaction into the velocity/spend windows. `EXACTLY_ONCE_V2` wraps the read-process-write cycle (consume from `transaction.initiated`, update RocksDB state, produce to `fraud.alert`) in a Kafka transaction, so either the whole cycle commits or none of it does — the windowed counts stay accurate even across crashes and rebalances.

**Why is `application-id` also the consumer group?** Kafka Streams uses the `application.id` as the underlying consumer group ID for all internal consumers reading `transaction.initiated`/`account.created`. All instances sharing the same `application.id` form one Streams application and share partition assignment — same as `group-id` does for a plain consumer.

---

## 9. Kafka Cluster Architecture

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

`fraud-detection-service`'s Kafka Streams application uses the same consumer-group mechanics internally (its `application.id` is the group), plus a `GlobalKTable` consumer that reads *all* partitions of `account.created` on every instance (global tables are not partition-assigned like regular consumer groups).

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

## 10. Topic Design

| Topic | Partitions | Replication | Key | Purpose |
|---|---|---|---|---|
| `account.created` | 3 | 2 | account_id | Reference data — seeded once by `AccountSeeder`, consumed as a `GlobalKTable` by fraud-detection-service |
| `transaction.initiated` | 3 | 2 | account_id | New transactions — ordered per account, consumed by both notification-service and fraud-detection-service |
| `transaction.completed` | 3 | 2 | account_id | Reserved — not currently produced or consumed by any service |
| `transaction.failed` | 3 | 2 | account_id | Reserved — not currently produced or consumed by any service |
| `fraud.alert` | 3 | 2 | account_id | Fraud detection results — produced by fraud-detection-service's three rules |
| `transaction.dlq` | 1 | 2 | original key | Dead letter queue — single partition for ordered inspection |

### Why account_id as Partition Key?

```
With account_id as key:
  ACC001 → always Partition 2
  ACC001 tx1, tx2, tx3 → all Partition 2, in order

Without a key (round-robin):
  ACC001 tx1 → Partition 0, tx2 → Partition 1, tx3 → Partition 2
  Consumed by different threads in unpredictable order
  Fraud detection sees events out of sequence → wrong velocity/spend counts
```

This matters even more now than in earlier phases — the Kafka Streams velocity and spend windows in `fraud-detection-service` depend on per-account ordering to produce correct counts.

### Why DLQ Has 1 Partition?

DLQ is for manual inspection. Ordering and throughput are not concerns. Single partition = simple sequential inspection of failed messages.

---

## 11. Database Schema

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
-- payload now holds a base64-encoded Avro wire payload (magic byte + schema id + binary), quoted as a JSON string
CREATE TABLE outbox (
    id            UUID PRIMARY KEY,
    topic         VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255),
    payload       JSONB NOT NULL,
    published     BOOLEAN NOT NULL DEFAULT false,
    published_at  TIMESTAMP,
    retry_count   INT NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TIMESTAMP NOT NULL
);

CREATE INDEX idx_outbox_unpublished ON outbox(created_at) WHERE published = false;

-- Idempotency guard — owned by notification-service
CREATE TABLE processed_events (
    event_id       UUID PRIMARY KEY,
    consumer_group VARCHAR(255) NOT NULL,
    processed_at   TIMESTAMP NOT NULL
);

-- No fraud_alerts table exists. fraud-detection-service is Postgres-free —
-- fraud state lives entirely in Kafka (fraud.alert topic + RocksDB/changelog
-- topics for windowed aggregates), not in a relational table. Earlier revisions
-- of this doc mentioned a fraud_alerts table for a Phase 6 that hadn't landed yet;
-- the service that did land takes a purely Kafka-native approach instead.
```

### Why JSONB for outbox payload?

`JSONB` in Postgres is binary JSON — queryable, indexable, validated at insert time. Using `TEXT`/`VARCHAR` would store the JSON as a plain string with no validation or queryability. This still applies even though the payload is now a base64-encoded Avro blob rather than a readable JSON object — it's stored as a JSON *string* value inside the `jsonb` column.

**Hibernate gotcha:** Hibernate sends `String` as `VARCHAR` by default. Must annotate with both:

```java
@Column(nullable = false, columnDefinition = "jsonb")
@org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
private String payload;
```

---

## 12. Runtime Data Flows

### Flow 1: Happy Path — Transaction Created Successfully

```
1. POST /api/transactions
   body: { accountId, amount, type, description }

2. TransactionController.initiateTransaction()
   → TransactionService.initiateTransaction()
   → @Transactional opens DB connection

3. transactionRepository.save(transaction) → INSERT queued

4. Build TransactionInitiatedEvent (Avro) once, eventId = UUID A

5. kafkaAvroSerializer.serialize(...) → raw Avro wire bytes
   → base64-encode → outboxRepository.save(outbox row, published=false)

6. @Transactional commits → transaction + outbox row atomic

7. transactionProducer.publishTransactionInitiated(avroEvent)
   → publishes IMMEDIATELY via avroKafkaTemplate, same eventId A
   → this happens synchronously in the request thread, NOT gated by the outbox

8. HTTP 200 → Transaction entity returned to client

9. OutboxPoller fires (within 1 second)
   → SELECT unpublished FOR UPDATE SKIP LOCKED → finds the outbox row from step 5
   → decodes base64 → raw Avro bytes → bytesKafkaTemplate.send(...).get()
   → publishes a SECOND message, this time with an entirely new eventId B
     (the outbox payload was built with a different eventId than step 7 rebuilds — actually
      the SAME eventId A is in the outbox payload since it was serialized before either publish;
      see Section 18 for the precise duplication mechanics)
   → UPDATE outbox SET published=true

10. notification-service consumer receives BOTH messages (from steps 7 and 9)
    on the account's partition, processes each independently
    (idempotency guard does not fully collapse them — see Section 18)

11. fraud-detection-service's Kafka Streams topology also sees BOTH messages,
    inflating velocity/spend window counts for that account
```

**Read [Section 18](#18-known-issues--things-to-clean-up) before relying on this flow for anything beyond learning purposes** — step 7 undermines the atomicity guarantee the outbox pattern (steps 3–6, 9) is designed to provide.

### Flow 2: Kafka Down During Outbox Poll

```
1. Transaction saved to DB + outbox (published=false)
2. OutboxPoller fires → bytesKafkaTemplate.send().get() → TimeoutException
3. outbox.retry_count++, outbox.last_error = "timeout"
4. Outbox entry stays published=false
5. Kafka comes back
6. OutboxPoller fires again → send succeeds → outbox.published=true
```

Note: the *direct* publish in `TransactionService` (step 7 in Flow 1) has no such retry — if Kafka is down at request time, that publish silently fails (logged, not retried) while the outbox path still recovers on its own schedule.

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

### Flow 4: Fraud Detection — High-Value Transaction

```
1. Kafka Streams reads a TransactionInitiatedEvent from transaction.initiated

2. .join(accountTable, ...) looks up the account's current AccountEvent
   from the GlobalKTable (built from account.created, seeded at transaction-service startup)

3. AccountTier.fromString(account.getTier()).getHighValueThreshold()
   → e.g. PREMIUM → ₹1,00,000

4. transaction.getAmount() > threshold?
   → yes: build FraudAlertEvent(alertType=HIGH_VALUE, ...) → publish to fraud.alert
   → no: transaction passes through, no alert

(Velocity and rapid-spend rules run in parallel on the same raw transaction stream,
 independent of the join — see Section 8.4)
```

### Flow 5: Consumer Rebalance — No Message Loss

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

### Object Transformation Chain (Happy Path, Avro)

```
HTTP Request JSON
  ↓ Jackson deserialization
InitiateTransactionRequest (record)
  ↓ TransactionService
Transaction (JPA entity) → Postgres
  ↓ TransactionInitiatedEvent.newBuilder()...build()  (Avro-generated class)
TransactionInitiatedEvent (Avro SpecificRecord, in-memory)
  ├─→ kafkaAvroSerializer.serialize() → raw Avro bytes → base64 → outbox.payload (JSONB)
  │     ↓ OutboxPoller: base64-decode → bytesKafkaTemplate (ByteArraySerializer, passthrough)
  │   Kafka message bytes  [magic byte][schema id][avro binary]
  └─→ avroKafkaTemplate (KafkaAvroSerializer, re-serializes + registers schema)
        Kafka message bytes  [magic byte][schema id][avro binary]
  ↓ KafkaAvroDeserializer (consumer side, specific.avro.reader=true)
TransactionInitiatedEvent (Avro SpecificRecord, typed — notification-service & fraud-detection-service)
  ↓ processed / windowed / joined
ack / fraud.alert
```

---

## 13. Key Design Decisions

### Decision 1: Outbox Pattern over Direct Publish

**Problem:** No atomic operation spans Postgres and Kafka.
**Decision:** Write to outbox table in same DB transaction. Separate poller publishes to Kafka.
**Tradeoff:** Up to 1 second latency between transaction creation and Kafka event.
**Alternative:** Debezium CDC (sub-millisecond, but adds operational complexity).
**Status:** currently undermined by a direct publish also happening in the same request path — see [Section 18](#18-known-issues--things-to-clean-up).

### Decision 2: Multiple KafkaTemplate Beans, One Per Serialization Strategy

**Problem:** A pre-serialized payload sent through the wrong serializer gets double-encoded or corrupted (e.g., raw Avro bytes through `JsonSerializer`).
**Decision:** One `KafkaTemplate` bean per serializer — `objectKafkaTemplate` (JSON), `avroKafkaTemplate` (typed Avro objects, schema managed by Confluent client), `stringKafkaTemplate` (pre-serialized JSON strings), `bytesKafkaTemplate` (pre-serialized raw bytes, passthrough).
**Why not one:** Type erasure — all collapse to raw `KafkaTemplate` at runtime. `@Qualifier` required for disambiguation.

### Decision 3: Manual Offset Commit (MANUAL_IMMEDIATE)

**Problem:** Auto-commit fires on timer — can commit offsets for messages still being processed.
**Decision:** Commit offset only after successful processing AND DB write to processed_events.
**Tradeoff:** At-least-once delivery — message can be redelivered on failure. Idempotency guard handles duplicates.

### Decision 4: Idempotency via DB Primary Key

**Problem:** At-least-once delivery means duplicate messages are inevitable.
**Decision:** `processed_events` table with `event_id` as primary key. DB constraint is the enforcer — not application logic.
**Tradeoff:** Extra DB write per message. Acceptable for banking — correctness over throughput.
**Caveat:** only catches duplicates that share an `eventId` — see the dual-publish issue in [Section 18](#18-known-issues--things-to-clean-up).

### Decision 5: kafka1 as Sole Controller

**Problem:** 2-node Raft with both as voters requires both alive simultaneously — startup deadlock.
**Decision:** kafka1 is controller + broker. kafka2 is broker only.
**Tradeoff:** kafka1 failure = control plane outage. Acceptable for dev. Production needs 3 controller nodes.

### Decision 6: `fixedDelay` over `fixedRate` for OutboxPoller

**Problem:** `fixedRate` fires every N ms regardless of previous execution time. If previous poll takes 30s (Kafka slow), next fires before previous completes — concurrent pollers, lock contention.
**Decision:** `fixedDelay=1000` — waits 1s after previous execution COMPLETES before starting next.
**Result:** One poll cycle at a time, no overlap.

### Decision 7: Avro + Schema Registry over JSON

**Problem:** JSON gives zero compile-time or deploy-time guarantee that producer and consumer agree on message shape; a producer field rename silently breaks every consumer at runtime.
**Decision:** All three services now speak Avro, validated against a shared Schema Registry, with generated typed classes from `.avsc` files.
**Tradeoff:** Extra moving part (Schema Registry must be up for producers/consumers to work), extra build step (avro-maven-plugin codegen), less human-readable wire format (binary, not inspectable in Kafka UI without schema-aware tooling — Kafka UI handles this via its own registry connection).
**Alternative considered implicitly:** Protobuf (similar guarantees, different ecosystem) — Avro chosen for tighter Confluent tooling integration.

### Decision 8: `GlobalKTable` for Account Reference Data in fraud-detection-service

**Problem:** Fraud rules need per-transaction account context (tier) without depending on Postgres from a Kafka Streams app.
**Decision:** Publish account data onto `account.created` (via `AccountSeeder`) and join against it as a `GlobalKTable` inside the Streams topology.
**Tradeoff:** Reference data lives in two places (Postgres `accounts` table for transaction-service/init.sql, and `account.created` topic for fraud-detection-service) with no automated sync — see [6.9](#69-account-seeder).

### Decision 9: `EXACTLY_ONCE_V2` for the Fraud Detection Streams App

**Problem:** At-least-once stream processing could double-count a transaction into a velocity or spend window on a crash-and-retry.
**Decision:** `PROCESSING_GUARANTEE_CONFIG = EXACTLY_ONCE_V2` wraps consume-process-produce in a Kafka transaction.
**Tradeoff:** Slightly higher latency and broker-side transaction coordinator load; correctness is prioritized since these are fraud thresholds.

---

## 14. Naming Conventions

| Location | Pattern | Example | Rationale |
|---|---|---|---|
| `domain/` | `<Entity>.java` | `Transaction.java` | JPA entity — matches table name |
| `domain/` | `<Entity>Status.java` | `TransactionStatus.java` | Enum for entity status field |
| `domain/` | `<Entity>Type.java` | `TransactionType.java` | Enum for entity type field |
| `src/main/avro/` | `<Entity><Action>Event.avsc` | `TransactionInitiatedEvent.avsc` | Avro schema — source of truth for the generated Kafka payload class |
| `repository/` | `<Entity>Repository.java` | `TransactionRepository.java` | Spring Data convention |
| `service/` | `<Entity>Service.java` | `TransactionService.java` | Business logic orchestration |
| `producer/` | `<Entity>Producer.java` | `TransactionProducer.java` | Kafka message publisher |
| `consumer/` | `<Entity>Consumer.java` | `TransactionConsumer.java` | Kafka message listener |
| `outbox/` | `OutboxPoller.java` | — | Scheduled outbox publisher |
| `dlq/` | `DlqProducer.java` | — | Dead letter queue publisher |
| `seeder/` | `<Entity>Seeder.java` | `AccountSeeder.java` | One-time (per-startup) reference-data publisher |
| `streams/` | `<Domain>Topology.java` | `FraudDetectionTopology.java` | Kafka Streams DAG definition |
| `config/` | `Kafka<Role>Config.java` | `KafkaProducerConfig.java`, `KafkaStreamsConfig.java` | Spring @Configuration |
| Bean names | `"<type>KafkaTemplate"` | `"avroKafkaTemplate"`, `"bytesKafkaTemplate"` | Disambiguates same raw type |
| Topics | `<domain>.<action>` | `transaction.initiated`, `fraud.alert` | Dot-separated, past tense action |
| Consumer groups | `<service-name>` | `notification-service`, `fraud-detection-service` | Matches Spring app name / Streams `application.id` |

---

## 15. Project Structure

```
bankstream/
├── docker-compose.yml              ← all infrastructure (Kafka x2, Schema Registry, Kafka UI, Postgres)
├── init.sql                        ← DB schema + seed data (runs on first Postgres boot)
├── pom.xml                         ← parent Maven POM (groupId: com.bankstream), Avro + Confluent deps
├── docs/
│   ├── Arcitecture.md              ← this file
│   └── BankStream — Kafka & Spring Boot Reference.md
│
├── transaction-service/            ← port 8090
│   ├── pom.xml                     ← avro-maven-plugin bound to generate-sources
│   └── src/main/
│       ├── avro/
│       │   ├── TransactionInitiatedEvent.avsc
│       │   └── AccountEvent.avsc
│       ├── java/com/bankstream/transaction/
│       │   ├── TransactionServiceApplication.java
│       │   ├── config/KafkaProducerConfig.java
│       │   ├── controller/TransactionController.java
│       │   ├── domain/{Transaction,TransactionStatus,TransactionType,Outbox}.java
│       │   ├── outbox/OutboxPoller.java
│       │   ├── producer/TransactionProducer.java
│       │   ├── repository/{TransactionRepository,OutboxRepository}.java
│       │   ├── seeder/AccountSeeder.java
│       │   └── service/TransactionService.java
│       └── resources/application.yml
│   └── target/generated-sources/avro/  ← generated Avro classes (gitignored in a normal setup)
│
├── notification-service/           ← port 8091
│   ├── pom.xml
│   └── src/main/
│       ├── avro/TransactionInitiatedEvent.avsc     ← local copy, same namespace
│       ├── java/com/bankstream/notification/
│       │   ├── NotificationServiceApplication.java
│       │   ├── config/{KafkaConsumerConfig,KafkaProducerConfig}.java
│       │   ├── consumer/TransactionConsumer.java
│       │   ├── dlq/DlqProducer.java
│       │   ├── domain/ProcessedEvent.java
│       │   ├── repository/ProcessedEventRepository.java
│       │   └── service/NotificationService.java
│       └── resources/application.yml
│
└── fraud-detection-service/        ← port 8092
    ├── pom.xml                     ← kafka-streams, kafka-streams-avro-serde
    └── src/main/
        ├── avro/
        │   ├── TransactionInitiatedEvent.avsc      ← local copy
        │   ├── AccountEvent.avsc                   ← local copy
        │   └── FraudAlertEvent.avsc                ← source of truth (only producer)
        ├── java/com/bankstream/fraud/
        │   ├── FraudDetectionApplication.java
        │   ├── config/KafkaStreamsConfig.java
        │   ├── model/AccountTier.java
        │   └── streams/FraudDetectionTopology.java
        └── resources/application.yml
```

---

## 16. How to Add a New Event Flow (Step-by-Step)

Example: publish `transaction.completed` when a transaction settles.

### Step 1: Create the Avro schema

**File:** `transaction-service/src/main/avro/TransactionCompletedEvent.avsc`

```json
{
  "type": "record",
  "name": "TransactionCompletedEvent",
  "namespace": "com.bankstream.transaction.event.avro",
  "fields": [
    {"name": "eventId", "type": "string"},
    {"name": "transactionId", "type": "string"},
    {"name": "accountId", "type": "string"},
    {"name": "completedAt", "type": "long"}
  ]
}
```

Run `mvn generate-sources -pl transaction-service` (or a full build) to generate the Java class before referencing it in code.

### Step 2: Write the outbox entry inside the existing `@Transactional` method

```java
byte[] avroBytes = kafkaAvroSerializer.serialize("transaction.completed", completedEvent);
outboxRepository.save(Outbox.builder()
    .topic("transaction.completed")
    .partitionKey(accountId.toString())
    .payload("\"" + Base64.getEncoder().encodeToString(avroBytes) + "\"")
    .build());
```

Do **not** also call a direct producer publish alongside the outbox write in the same method — that's the exact anti-pattern flagged in [Section 18](#18-known-issues--things-to-clean-up). Let the `OutboxPoller` be the only path to Kafka for this event.

### Step 3: Verify the topic exists

```bash
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 --describe --topic transaction.completed
```

If not: create it (auto-create is disabled — see [Section 17](#17-build--run-reference)).

### Step 4: Copy the `.avsc` into any consuming service and generate its class

Same schema, same `namespace`, copied into e.g. `fraud-detection-service/src/main/avro/`. Wire a new `@KafkaListener` (or extend the Streams topology with `builder.stream("transaction.completed", ...)`) following the existing patterns in `TransactionConsumer` or `FraudDetectionTopology`.

---

## 17. Build & Run Reference

### Prerequisites

- Docker Desktop running
- Java 17
- Maven 3.8+

### Start Infrastructure

```bash
cd bankstream
docker compose up -d
# Wait ~60 seconds
docker compose ps  # kafka1, kafka2, schema-registry, kafka-ui, postgres should all show "healthy"
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

### Build (generates Avro classes as part of the build)

```bash
cd bankstream
mvn clean install
# avro-maven-plugin runs in the generate-sources phase for each module —
# no separate "generate avro" step needed, it's part of the normal Maven lifecycle
```

### Run Services

```bash
# Terminal 1 — also seeds account.created on startup via AccountSeeder
mvn spring-boot:run -pl transaction-service

# Terminal 2
mvn spring-boot:run -pl notification-service

# Terminal 3 — Kafka Streams app, needs Schema Registry + account.created + transaction.initiated
mvn spring-boot:run -pl fraud-detection-service
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

# High-value fraud trigger (PREMIUM account threshold ₹1,00,000)
curl -X POST http://localhost:8090/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"accountId":"a0000001-0000-0000-0000-000000000001","amount":150000.00,"type":"DEBIT","description":"Should trigger HIGH_VALUE alert"}'

# Velocity fraud trigger — fire this 3+ times within 5 minutes for the same account
curl -X POST http://localhost:8090/api/transactions \
  -H "Content-Type: application/json" \
  -d '{"accountId":"a0000001-0000-0000-0000-000000000002","amount":500.00,"type":"DEBIT","description":"velocity test"}'
```

### Useful Kafka / Schema Registry Commands

```bash
# Describe topic partition distribution
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 --describe --topic transaction.initiated

# Check consumer group lag (notification-service, or fraud-detection-service's application-id)
docker exec -it bankstream-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 --describe --group notification-service
docker exec -it bankstream-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 --describe --group fraud-detection-service

# List registered schemas
curl -s http://localhost:8081/subjects | jq

# Get the latest schema for a subject (subject naming default: <topic>-value)
curl -s http://localhost:8081/subjects/transaction.initiated-value/versions/latest | jq

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

# Inspect fraud-detection-service's local RocksDB state dir (velocity/spend window stores)
ls -la /tmp/kafka-streams/fraud-detection

# Wipe everything and start fresh (also wipes Kafka Streams local state — safe, rebuilds from changelog)
docker compose down -v && docker compose up -d
```

---

## 18. Known Issues / Things to Clean Up

These are honest notes about the current state of the code, written so they don't get silently "discovered" again later.

### 18.1 `TransactionService` publishes `transaction.initiated` twice per request

**Where:** `transaction-service/src/main/java/com/bankstream/transaction/service/TransactionService.java`, method `initiateTransaction(...)`.

**What happens:** the method (a) writes an outbox row containing the Avro-serialized event (published asynchronously, ~1s later, by `OutboxPoller`), **and** (b) calls `transactionProducer.publishTransactionInitiated(avroEvent)` directly, synchronously, in the same method, before returning. Both sends target the exact same topic (`transaction.initiated`), same partition key, and — since the same `avroEvent` object (built once, with one `eventId`) is used for both — actually the **same `eventId`**, meaning the two messages are near-identical, differing only in exact serialization path (`bytesKafkaTemplate` sending pre-serialized bytes vs. `avroKafkaTemplate` re-serializing the object through `KafkaAvroSerializer` directly).

**Why this matters:**
- It defeats the atomicity guarantee the outbox pattern exists to provide — step (b) is a direct, non-transactional Kafka call with no retry-via-outbox if it fails at request time (it just logs and moves on).
- Every transaction currently produces two messages on the topic. Because both carry the *same* `eventId`, `notification-service`'s idempotency guard (dedup by `eventId`) actually *does* catch the second one as a duplicate — so end-user-visible behavior (one notification per transaction) looks correct by luck, not by design.
- `fraud-detection-service` has **no idempotency concept at all** — its velocity and spend window counts are Kafka Streams aggregates over the raw stream, so they count both messages. Every account will trip the velocity rule (≥3 in 5 minutes) roughly twice as fast as intended, and hourly spend totals will read double actual spend.

**Likely origin:** this looks like leftover code from migrating the outbox payload from JSON to Avro — the original JSON-era code (still visible, commented out, directly above this block in the source) only wrote to the outbox and let the poller be the sole publisher. The direct-publish call appears to have been added during Avro work (possibly for quick manual testing of the Avro producer path) and never removed.

**What to do about it:** pick one path. If you want to keep the outbox's atomicity guarantee (recommended, matches [Decision 1](#13-key-design-decisions)), delete the `transactionProducer.publishTransactionInitiated(avroEvent)` call from `TransactionService` entirely — the `OutboxPoller` is sufficient. `TransactionProducer` itself and the `avroKafkaTemplate` bean can either be deleted or kept around purely as a "here's how you'd publish Avro directly, for comparison" teaching artifact — just don't call it from the main write path.

### 18.2 Unused/legacy `KafkaTemplate` beans

`objectKafkaTemplate` (JSON) and `stringKafkaTemplate` (String passthrough) in `transaction-service/config/KafkaProducerConfig.java` are leftovers from the pre-Avro phases and are not referenced anywhere in current code. Harmless to leave as reference/teaching material (this repo's whole point is showing the evolution of these patterns) but worth knowing they're dead code, not active configuration.

### 18.3 `.avsc` files are hand-copied across modules, not shared

See [Section 5, "Where Each Schema Lives"](#5-schema-registry--avro-serialization) — `TransactionInitiatedEvent.avsc` and `AccountEvent.avsc` exist as separate file copies in multiple modules. No tooling currently enforces they stay identical. If you change a schema, grep for the schema name across all three service directories and update every copy.

### 18.4 `transaction.completed` / `transaction.failed` topics exist in config but nothing publishes to them yet

Both are created by the topic-creation script in [Section 17](#17-build--run-reference) and listed in the [Topic Design table](#10-topic-design), but no producer or consumer code references them yet. They're reserved for a future flow (see [Section 16](#16-how-to-add-a-new-event-flow-step-by-step) for the pattern to follow when adding one).

### 18.5 No `fraud_alerts` Postgres table, despite the DB schema section in earlier doc revisions implying one

`fraud-detection-service` never got a JPA/Postgres layer — it's intentionally Kafka-native (state in RocksDB + changelog topics, alerts land on the `fraud.alert` topic only). If you want alerts queryable outside Kafka (e.g., for a dashboard), you'd add a consumer that reads `fraud.alert` and writes to Postgres — that consumer doesn't exist yet.

---

## 19. Common Gotchas

| # | Gotcha | Root Cause | Fix |
|---|--------|------------|-----|
| 1 | `column "payload" is of type jsonb but expression is of type character varying` | Hibernate sends String as VARCHAR | Add `@JdbcTypeCode(SqlTypes.JSON)` to payload field |
| 2 | `Could not instantiate com.fasterxml.jackson.databind.JsonSerializer` | Wrong import — Jackson's abstract class | Import `org.springframework.kafka.support.serializer.JsonSerializer` |
| 3 | `Illegal attempt to set lock mode for a native query` | `@Lock` on native query | Remove `@Lock` — `FOR UPDATE SKIP LOCKED` in SQL is enough |
| 4 | `required a bean of type KafkaTemplate that could not be found` | Multiple beans, type erasure, Spring can't choose | Add `@Bean("name")` + `@Qualifier("name")` |
| 5 | `constructor X is already defined` | `@RequiredArgsConstructor` + manual constructor conflict | Remove `@RequiredArgsConstructor` when writing constructor manually with `@Qualifier` |
| 6 | `UnknownHostException: kafka2` | Both brokers as Raft voters — kafka1 tries to reach kafka2 before it starts | Make kafka2 broker-only, kafka1 sole controller |
| 7 | Consumer receives `String`/`Map` instead of the typed Avro class | `specific.avro.reader` not set to `true`, or wrong deserializer configured | Set `specific.avro.reader=true` and use `KafkaAvroDeserializer` |
| 8 | Consumer stuck on same offset forever | Not acking after DLQ routing | Always `acknowledgment.acknowledge()` even after DLQ — original topic must move on |
| 9 | Leader imbalance after broker recovery | Kafka doesn't auto-restore preferred leader | Run `kafka-leader-election --election-type preferred` manually |
| 10 | Outbox table growing unbounded | No cleanup job | Run `DELETE FROM outbox WHERE published=true AND published_at < now() - INTERVAL '7 days'` nightly |
| 11 | `auto_create_topics_enable=false` causes `NoSuchTopicException` | Topic not created before producer sends | Create topics explicitly before starting services — see build reference above |
| 12 | `application.yml` kafka settings ignored | Programmatic `@Configuration` bean overrides auto-config entirely | Set ALL settings in the bean — nothing is inherited from yml |
| 13 | `DataIntegrityViolationException` on processed_events insert | Race condition — two instances processed same message | Catch `DVE`, log, ack and continue — this is expected and safe |
| 14 | `version` attribute obsolete warning in docker compose | Old `version: '3.8'` top-level key | Remove the `version:` line — Docker Compose v2 doesn't need it |
| 15 | `org.apache.kafka.common.errors.SerializationException: Error registering Avro schema` | Schema Registry unreachable, or a genuinely incompatible schema change (e.g. removed a required field) | Check Schema Registry health (`curl localhost:8081/subjects`); if it's a real incompatibility, evolve the schema correctly — new fields need `["null", type]` + `"default": null` |
| 16 | `KafkaAvroDeserializer` throws `SerializationException: Unknown magic byte!` | Trying to deserialize non-Avro bytes as Avro (e.g. an old JSON message still sitting in a topic from before the Avro migration, or the raw outbox base64 bytes were mis-decoded) | Wipe the topic (`docker compose down -v`) when switching serialization formats on an existing topic — old and new formats cannot coexist on one topic |
| 17 | Fraud detection velocity/spend counts look roughly 2x what you'd expect | The dual-publish issue — see [Section 18.1](#181-transactionservice-publishes-transactioninitiated-twice-per-request) | Remove the direct `transactionProducer.publishTransactionInitiated()` call from `TransactionService`, rely on the outbox only |
| 18 | `fraud-detection-service` doesn't see any accounts / `HIGH_VALUE` rule never fires | `AccountSeeder` only runs when `transaction-service` starts up — if fraud-detection-service was started first, or Postgres/Kafka weren't ready when the seeder ran, `account.created` may be empty | Restart `transaction-service` to re-run the seeder (idempotent — re-publishing the same keys is safe), confirm with `kafka-console-consumer --topic account.created --from-beginning` |
| 19 | Kafka Streams app fails to start with `Missing source topic(s)` | `transaction.initiated` or `account.created` doesn't exist yet when fraud-detection-service starts (auto-create is disabled) | Create topics first (see [Section 17](#17-build--run-reference)) before starting fraud-detection-service |
| 20 | `SpecificAvroSerde` `ClassCastException` inside the Streams topology | Local `.avsc` copy in `fraud-detection-service` has drifted from the one `transaction-service` actually produced with | Diff the `.avsc` files across modules — see [Section 18.3](#183-avsc-files-are-hand-copied-across-modules-not-shared) |
