# BankStream

A real-time banking event processing system built to learn and implement Kafka concepts from basics to production-grade patterns.

## What this project covers

| Phase | Concept | Status |
|---|---|---|
| 1 | Docker Compose — Kafka (KRaft), Schema Registry, Kafka UI, Postgres | ✅ Done |
| 2 | Producer/consumer basics — acks, idempotence, partition keys, offset management | ✅ Done |
| 3 | Outbox pattern — guaranteed Kafka delivery with DB atomicity | ✅ Done |
| 4 | Dead letter queue — retry with exponential backoff, poison pill handling | ✅ Done |
| 5 | Avro + Schema Registry — schema evolution, backward compatibility | 🔲 Pending |
| 6 | Kafka Streams — aggregations, windowing, stream-table joins, fraud detection | 🔲 Pending |

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
  → OutboxPoller publishes to Kafka every 1s with retry

Kafka (2-broker cluster)
  kafka1 → controller + broker
  kafka2 → broker only
  Topics: transaction.initiated, transaction.completed,
          transaction.failed, fraud.alert, transaction.dlq

notification-service (port 8091)
  → Consumes transaction.initiated
  → Manual offset commit (at-least-once)
  → Idempotency guard via processed_events table
  → Retry with exponential backoff (1s, 2s, 4s)
  → Routes to transaction.dlq after 3 failed attempts

fraud-detection-service (port 8092) [Phase 6]
  → Kafka Streams
  → Aggregates spend per account (1hr tumbling window)
  → Joins with account GlobalKTable for tier enrichment
  → Detects 3+ transactions in 5 min → produces fraud.alert
```

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

### 5. Access Kafka UI

Open `http://localhost:8080` — shows brokers, topics, consumer groups, messages, schemas.

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
├── pom.xml                               ← parent pom
├── transaction-service/
│   ├── pom.xml
│   └── src/main/java/com/bankstream/transaction/
│       ├── config/KafkaProducerConfig.java
│       ├── controller/TransactionController.java
│       ├── domain/
│       │   ├── Transaction.java
│       │   ├── TransactionStatus.java
│       │   ├── TransactionType.java
│       │   └── Outbox.java
│       ├── event/TransactionInitiatedEvent.java
│       ├── outbox/OutboxPoller.java
│       ├── producer/TransactionProducer.java
│       ├── repository/
│       │   ├── TransactionRepository.java
│       │   └── OutboxRepository.java
│       └── service/TransactionService.java
└── notification-service/
    ├── pom.xml
    └── src/main/java/com/bankstream/notification/
        ├── config/
        │   ├── KafkaConsumerConfig.java
        │   └── KafkaProducerConfig.java
        ├── consumer/TransactionConsumer.java
        ├── dlq/DlqProducer.java
        ├── domain/ProcessedEvent.java
        ├── repository/ProcessedEventRepository.java
        └── service/NotificationService.java
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

### Two KafkaTemplate beans
`objectKafkaTemplate` (JsonSerializer) used by TransactionProducer for typed event objects. `stringKafkaTemplate` (StringSerializer) used by OutboxPoller for raw JSON strings already stored in the outbox table. Sending a JSON string through JsonSerializer would double-serialize it.

### Manual offset commit + idempotency
Consumers use `AckMode.MANUAL_IMMEDIATE` — offset committed only after successful processing and DB write. Idempotency guard in `processed_events` table handles redeliveries. Race condition between instances handled by catching `DataIntegrityViolationException` on primary key insert.

### DLQ after 3 retries with exponential backoff
Retry attempts: 1s → 2s → 4s backoff. After exhaustion, message routed to `transaction.dlq` and original offset acknowledged. Without the ack, consumer stalls forever on the poison pill offset.

## Useful commands

```bash
# Check consumer group lag
docker exec -it bankstream-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --describe --group notification-service

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

Detailed notes on every concept covered (Kafka internals, Kafka Streams, outbox pattern, DLQ, debugging) are in `bankstream-kafka-notes.md` (Obsidian format).