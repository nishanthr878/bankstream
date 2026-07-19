# BankStream

A real-time banking event processing system built to learn and implement Kafka concepts from basics to production-grade patterns.

## What this project covers

| Phase | Concept | Status |
|---|---|---|
| 1 | Docker Compose — Kafka (KRaft), Schema Registry, Kafka UI, Postgres | ✅ Done |
| 2 | Producer/consumer basics — acks, idempotence, partition keys, offset management | ✅ Done |
| 3 | Outbox pattern — guaranteed Kafka delivery with DB atomicity | ✅ Done |
| 4 | Dead letter queue — retry with exponential backoff, poison pill handling | ✅ Done |
| 5 | Avro + Schema Registry — schema evolution, backward compatibility | ✅ Done |
| 6 | Kafka Streams — aggregations, windowing, stream-table joins, fraud detection | ✅ Done |

## Tech stack

- Java 17
- Spring Boot 3.2.5
- Apache Kafka 3.6 (KRaft mode — no ZooKeeper)
- Confluent Schema Registry 7.6
- PostgreSQL 16
- Docker + Docker Compose

## Architecture

```
transaction-service (port 8090)
  → REST API accepts transaction requests
  → Writes transaction + outbox entry in single DB transaction
  → OutboxPoller publishes to Kafka every 1s with retry (Avro, via Schema Registry)
  → AccountSeeder publishes seed accounts to account.created on startup

Schema Registry (port 8081)
  → Stores Avro schemas for transaction.initiated, account.created, fraud.alert
  → Enforces BACKWARD compatibility on every schema change

Kafka (2-broker cluster)
  kafka1 → controller + broker
  kafka2 → broker only
  Topics: transaction.initiated, account.created, transaction.completed,
          transaction.failed, fraud.alert, transaction.dlq

notification-service (port 8091)
  → Consumes transaction.initiated (typed Avro, specific.avro.reader=true)
  → Manual offset commit (at-least-once)
  → Idempotency guard via processed_events table
  → Retry with exponential backoff (1s, 2s, 4s)
  → Routes to transaction.dlq after 3 failed attempts

fraud-detection-service (port 8092)
  → Kafka Streams topology (spring-kafka @EnableKafkaStreams)
  → Joins transaction.initiated with account.created (GlobalKTable) for tier enrichment
  → HIGH_VALUE: single transaction exceeds tier threshold
  → VELOCITY: 3+ transactions for one account in a 5-min sliding window
  → RAPID_LARGE: total spend for one account exceeds ₹1,00,000 in a 1hr tumbling window
  → Exactly-once processing (EXACTLY_ONCE_V2), state in RocksDB + Kafka changelog topics
  → Produces fraud.alert
```

> **Known issue:** `TransactionService` currently publishes `transaction.initiated` both via the outbox *and* directly (leftover from the JSON→Avro migration) — see `docs/Arcitecture.md` §18 for details and the fix.

## Prerequisites

- Docker Desktop
- Java 17
- Maven 3.8+

## Getting started

### 1. Start infrastructure

```bash
cd bankstream
docker compose up -d
```

Wait ~60 seconds for all containers to be healthy.

```bash
docker compose ps
```

All 5 containers should show `healthy`.

### 2. Verify topics exist

```bash
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 --list
```

Expected topics:
```
account.created
fraud.alert
transaction.completed
transaction.dlq
transaction.failed
transaction.initiated
```

If topics are missing, create them:

```bash
docker exec -it bankstream-kafka1 kafka-topics --bootstrap-server kafka1:9092 --create --topic account.created --partitions 3 --replication-factor 2
docker exec -it bankstream-kafka1 kafka-topics --bootstrap-server kafka1:9092 --create --topic transaction.initiated --partitions 3 --replication-factor 2
docker exec -it bankstream-kafka1 kafka-topics --bootstrap-server kafka1:9092 --create --topic transaction.completed --partitions 3 --replication-factor 2
docker exec -it bankstream-kafka1 kafka-topics --bootstrap-server kafka1:9092 --create --topic transaction.failed --partitions 3 --replication-factor 2
docker exec -it bankstream-kafka1 kafka-topics --bootstrap-server kafka1:9092 --create --topic fraud.alert --partitions 3 --replication-factor 2
docker exec -it bankstream-kafka1 kafka-topics --bootstrap-server kafka1:9092 --create --topic transaction.dlq --partitions 1 --replication-factor 2
```

### 3. Run transaction-service

```bash
mvn spring-boot:run -pl transaction-service
```

### 4. Run notification-service

```bash
mvn spring-boot:run -pl notification-service
```

### 5. Run fraud-detection-service

```bash
mvn spring-boot:run -pl fraud-detection-service
```

Needs Schema Registry up and `transaction.initiated` / `account.created` created first (Kafka Streams won't auto-create source topics).

### 6. Access Kafka UI

Open `http://localhost:8080` — shows brokers, topics, consumer groups, messages, schemas.

### 7. Access Schema Registry

```bash
curl -s http://localhost:8081/subjects | jq
```

Lists registered schema subjects (`transaction.initiated-value`, `account.created-value`, `fraud.alert-value`, once each service has produced at least one message).

## API reference

### Create a transaction

```bash
curl -X POST http://localhost:8090/api/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "a0000001-0000-0000-0000-000000000001",
    "amount": 1500.00,
    "type": "DEBIT",
    "description": "ATM withdrawal"
  }'
```

### Trigger DLQ (amount ending in .99 simulates failure)

```bash
curl -X POST http://localhost:8090/api/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "a0000001-0000-0000-0000-000000000002",
    "amount": 999.99,
    "type": "DEBIT",
    "description": "This will fail and route to DLQ"
  }'
```

### Trigger a fraud alert (HIGH_VALUE — PREMIUM tier threshold is ₹1,00,000)

```bash
curl -X POST http://localhost:8090/api/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "a0000001-0000-0000-0000-000000000001",
    "amount": 150000.00,
    "type": "DEBIT",
    "description": "Should trigger a HIGH_VALUE fraud.alert"
  }'
```

Watch it land on `fraud.alert` in Kafka UI, or:

```bash
docker exec -it bankstream-kafka1 kafka-console-consumer \
  --bootstrap-server kafka1:9092 --topic fraud.alert --from-beginning
```
(raw output will be Avro binary — use Kafka UI's schema-aware message viewer to read it decoded)

### Seed accounts (pre-loaded by init.sql)

| Account ID | Number | Owner | Type | Tier | Balance |
|---|---|---|---|---|---|
| a0000001-0000-0000-0000-000000000001 | ACC001 | Nishanth Kumar | SAVINGS | PREMIUM | ₹50,000 |
| a0000001-0000-0000-0000-000000000002 | ACC002 | Ravi Shankar | CURRENT | STANDARD | ₹10,000 |
| a0000001-0000-0000-0000-000000000003 | ACC003 | Priya Mehta | SAVINGS | ELITE | ₹2,00,000 |

## Project structure

```
bankstream/
├── docker-compose.yml
├── init.sql                              ← DB schema + seed data
├── pom.xml                               ← parent pom, Avro + Confluent deps
├── docs/
│   ├── Arcitecture.md                    ← full architecture deep-dive (this repo's main reference doc)
│   └── BankStream — Kafka & Spring Boot Reference.md
├── transaction-service/
│   ├── pom.xml                           ← avro-maven-plugin bound to generate-sources
│   └── src/main/
│       ├── avro/
│       │   ├── TransactionInitiatedEvent.avsc
│       │   └── AccountEvent.avsc
│       └── java/com/bankstream/transaction/
│           ├── config/KafkaProducerConfig.java
│           ├── controller/TransactionController.java
│           ├── domain/
│           │   ├── Transaction.java
│           │   ├── TransactionStatus.java
│           │   ├── TransactionType.java
│           │   └── Outbox.java
│           ├── outbox/OutboxPoller.java
│           ├── producer/TransactionProducer.java
│           ├── repository/
│           │   ├── TransactionRepository.java
│           │   └── OutboxRepository.java
│           ├── seeder/AccountSeeder.java ← publishes seed accounts to account.created on startup
│           └── service/TransactionService.java
├── notification-service/
│   ├── pom.xml
│   └── src/main/
│       ├── avro/TransactionInitiatedEvent.avsc
│       └── java/com/bankstream/notification/
│           ├── config/
│           │   ├── KafkaConsumerConfig.java
│           │   └── KafkaProducerConfig.java
│           ├── consumer/TransactionConsumer.java
│           ├── dlq/DlqProducer.java
│           ├── domain/ProcessedEvent.java
│           ├── repository/ProcessedEventRepository.java
│           └── service/NotificationService.java
└── fraud-detection-service/
    ├── pom.xml                           ← kafka-streams, kafka-streams-avro-serde
    └── src/main/
        ├── avro/
        │   ├── TransactionInitiatedEvent.avsc
        │   ├── AccountEvent.avsc
        │   └── FraudAlertEvent.avsc
        └── java/com/bankstream/fraud/
            ├── config/KafkaStreamsConfig.java
            ├── model/AccountTier.java
            └── streams/FraudDetectionTopology.java
```

## Kafka cluster

| Broker | Role | Internal port | Host port |
|---|---|---|---|
| kafka1 | Controller + Broker | 9092 | 29092 |
| kafka2 | Broker only | 9094 | 29094 |

### Why kafka1 is the sole controller

Two-node Raft with both nodes as voters requires both to be up to form majority — chicken-and-egg on startup. kafka1 is the sole Raft voter, kafka2 is broker-only. This means:

- kafka2 down → cluster healthy, kafka1 serves all
- kafka1 down → control plane gone, no leader elections possible

Production fix: 3 controller nodes (majority = 2, tolerate 1 failure).

## Key design decisions

### Outbox pattern (Phase 3)
Transaction row and outbox entry written in a single DB transaction. Either both exist or neither does. The outbox poller handles Kafka publishing independently with retries. Solves the dual-write problem — `@Transactional` alone cannot span Postgres and Kafka.

### Multiple KafkaTemplate beans, one per serializer
`transaction-service` has four: `avroKafkaTemplate` (KafkaAvroSerializer, used by TransactionProducer), `bytesKafkaTemplate` (ByteArraySerializer, used by OutboxPoller for the pre-serialized Avro bytes sitting in the outbox table), plus legacy `objectKafkaTemplate`/`stringKafkaTemplate` (JSON-era, unused now). Sending pre-serialized bytes through the wrong serializer double-encodes or corrupts them — each payload shape gets its own template.

### Manual offset commit + idempotency
Consumers use `AckMode.MANUAL_IMMEDIATE` — offset committed only after successful processing and DB write. Idempotency guard in `processed_events` table handles redeliveries. Race condition between instances handled by catching `DataIntegrityViolationException` on primary key insert.

### DLQ after 3 retries with exponential backoff
Retry attempts: 1s → 2s → 4s backoff. After exhaustion, message routed to `transaction.dlq` and original offset acknowledged. Without the ack, consumer stalls forever on the poison pill offset.

### Avro + Schema Registry (Phase 5)
All three services exchange Avro instead of JSON. `.avsc` schema files under each module's `src/main/avro/` are compiled to typed Java classes by `avro-maven-plugin` at build time. Schema Registry (port 8081) enforces `BACKWARD` compatibility on every schema change and assigns each schema version a numeric ID embedded in every message's wire format (magic byte + schema ID + Avro binary).

### Kafka Streams fraud detection (Phase 6)
`fraud-detection-service` is a Kafka Streams app (`@EnableKafkaStreams`), not a `@KafkaListener` consumer — it needs state (windowed counts) and a stream-table join (`GlobalKTable` of `account.created`, keyed by account, joined against the `transaction.initiated` stream for tier lookups) that a plain listener can't give you without hand-building. Runs with `EXACTLY_ONCE_V2` so a crash mid-aggregation can't double-count a transaction into the fraud windows.

## Useful commands

```bash
# Check consumer group lag
docker exec -it bankstream-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --describe --group notification-service

# Check fraud-detection-service's Kafka Streams consumer group lag
docker exec -it bankstream-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --describe --group fraud-detection-service

# List registered Avro schemas
curl -s http://localhost:8081/subjects | jq

# Describe topic partition distribution
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 \
  --describe --topic transaction.initiated

# Trigger preferred leader election
docker exec -it bankstream-kafka1 kafka-leader-election \
  --bootstrap-server kafka1:9092 \
  --election-type preferred \
  --topic transaction.initiated --partition 1

# Check outbox table
docker exec -it bankstream-postgres psql -U bankstream -d bankstream \
  -c "SELECT id, topic, published, retry_count, last_error FROM outbox ORDER BY created_at DESC LIMIT 10;"

# Check processed events
docker exec -it bankstream-postgres psql -U bankstream -d bankstream \
  -c "SELECT * FROM processed_events ORDER BY processed_at DESC LIMIT 10;"

# Wipe everything and start fresh
docker compose down -v && docker compose up -d
```

## Reference notes

Full architecture deep-dive — every design decision explained with the failure it prevents, service-by-service code walkthroughs, Avro wire format, Kafka Streams topology, runtime data flows, and a running list of known issues — is in [`docs/Arcitecture.md`](docs/Arcitecture.md).

Additional Kafka & Spring Boot concept notes are in `docs/BankStream — Kafka & Spring Boot Reference.md`.