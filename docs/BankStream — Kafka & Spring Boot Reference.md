

## Table of contents

- [[#1. Kafka core concepts]]
- [[#2. Cluster architecture]]
- [[#3. Partitions & replication]]
- [[#4. Producer internals]]
- [[#5. Consumer internals]]
- [[#6. Delivery semantics]]
- [[#7. Kafka Streams]]
- [[#8. Outbox pattern]]
- [[#9. Dead letter queue]]
- [[#10. BankStream project structure]]
- [[#11. Docker Compose reference]]
- [[#12. Config reference]]
- [[#13. Debugging checklist]]

---

## 1. Kafka core concepts

> Kafka is a **distributed commit log**, not a queue. Messages stay after consumption. Consumers track position via offsets.

### Topic

- Logical category of messages (e.g. `transaction.initiated`)
- Split into **partitions** — each partition is an ordered, append-only log
- Ordering guaranteed **per partition only**

### Partition

```
topic: transaction.initiated (3 partitions)

Partition 0: [offset 0] [offset 1] [offset 2] ...
Partition 1: [offset 0] [offset 1] [offset 2] ...
Partition 2: [offset 0] [offset 1] [offset 2] ...
```

### Offset

- Integer position of next message to read
- Stored in `__consumer_offsets` internal topic
- Committed by consumer — not auto-committed in our setup

### Segment files (physical storage)

```
/kafka-logs/transaction.initiated-0/
  00000000000000000000.log        ← raw messages
  00000000000000000000.index      ← sparse offset → file position map
  00000000000000000000.timeindex  ← timestamp → offset map
```

**Read flow for offset 850:**

1. Binary search `.index` → nearest indexed offset ≤ 850
2. Seek to that file position in `.log`
3. Scan forward to offset 850

---

## 2. Cluster architecture

### Our BankStream setup

```
kafka1 → controller + broker (node 1)
kafka2 → broker only       (node 2)

Controller: handles leader election, cluster metadata (KRaft/Raft)
Broker:     serves producers/consumers, stores partition data
```

### KRaft (no ZooKeeper)

- Kafka manages its own metadata via Raft consensus
- `__cluster_metadata` topic replaces ZooKeeper
- `KAFKA_CONTROLLER_QUORUM_VOTERS` defines who votes in elections

### What happens when a broker dies

|Broker down|Result|
|---|---|
|`kafka2`|kafka1 keeps running. Kafka1 owns all leaders. ISR shrinks. Writes still work (min.isr=1).|
|`kafka1`|Controller gone. No new leader elections. kafka2 can serve existing leaders temporarily but no recovery possible. Full control plane outage.|

> **Why**: 2-node Raft with 1 voter = no fault tolerance on control plane. Production needs 3 controller nodes (majority = 2, tolerate 1 failure).

---

## 3. Partitions & replication

### Partition distribution (replication-factor=2)

```
transaction.initiated:
  Partition 0 → Leader: broker1, Replica: broker2, ISR: [1,2]
  Partition 1 → Leader: broker2, Replica: broker1, ISR: [1,2]
  Partition 2 → Leader: broker1, Replica: broker2, ISR: [1,2]
```

### ISR (In-Sync Replicas)

- Set of replicas caught up with leader within `replica.lag.time.max.ms` (10s default)
- `acks=all` waits for **all ISR** to confirm — not all replicas
- If ISR shrinks to 1, `acks=all` only waits for leader → false safety

### Production config

```
replication.factor=3
min.insync.replicas=2   ← broker-side: reject write if ISR < 2
acks=all                ← producer-side: wait for all ISR
```

### Partition key → which partition

```java
// With key: hash(key) % partitionCount
producer.send("transaction.initiated", "ACC001", payload)
// ACC001 always → same partition → ordering guaranteed per account

// Without key: round-robin → no ordering guarantee
producer.send("transaction.initiated", null, payload)
```

### Leader election after broker recovery

- Kafka does NOT automatically restore preferred leader
- Run manually: `kafka-leader-election --election-type preferred --topic X --partition N`
- Auto-rebalance: `auto.leader.rebalance.enable=true` (default), checks every 5 min

### Describe topic command

```bash
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 \
  --describe --topic transaction.initiated
```

---

## 4. Producer internals

### Send flow

```
producer.send("topic", key, value)
  → serialize
  → determine partition (key hash or round-robin)
  → add to RecordAccumulator (in-memory buffer per partition)
  → I/O thread drains buffer → sends batch to broker
  → broker acks → CompletableFuture completes
```

### Key producer configs

```yaml
acks: all                          # wait for all ISR
retries: 3                         # retry on transient failure
enable-idempotence: true           # dedup retries via sequence numbers
max.in.flight.requests.per.connection: 5   # must be ≤5 for idempotence
batch-size: 32768                  # 32KB batch
linger.ms: 5                       # wait 5ms to fill batch
```

### Why max.in.flight ≤ 5 with idempotence

Idempotence uses sequence numbers to detect duplicates. If >5 requests are in-flight simultaneously, network reordering can make seq=5 arrive before seq=3. Broker can't distinguish duplicate from late arrival. Cap at 5 = Kafka's tested safe limit.

### Two KafkaTemplate beans in BankStream

```java
// For typed events (TransactionProducer)
@Bean("objectKafkaTemplate")
KafkaTemplate<String, Object>  // uses JsonSerializer

// For outbox raw JSON strings (OutboxPoller)  
@Bean("stringKafkaTemplate")
KafkaTemplate<String, String>  // uses StringSerializer
```

> **Why two?** Outbox stores payload as JSON string in DB. Sending via JsonSerializer would double-serialize (JSON string inside JSON). StringSerializer sends as-is.

### @Qualifier usage

```java
// When two beans of same raw type exist, Spring can't autowire by type alone
// Type erasure: KafkaTemplate<String,Object> and KafkaTemplate<String,String>
// are both just KafkaTemplate at runtime
public TransactionProducer(
    @Qualifier("objectKafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate) {
```

---

## 5. Consumer internals

### Consumer group

```
Group: notification-service (3 consumers, 3 partitions)

Consumer A → Partition 0
Consumer B → Partition 1
Consumer C → Partition 2
```

- 1 partition → at most 1 consumer per group at a time
- Partitions = ceiling of parallelism (4th consumer sits idle with 3 partitions)
- Different groups = fully independent, each has own offset

### Rebalance triggers

- New consumer joins group
- Consumer dies (heartbeat timeout)
- Partition count changes
- Consumer calls `unsubscribe()`

> Rebalances = all consumption pauses. Production fix: Static Membership (`group.instance.id`)

### Poll loop

```java
// poll() does three things:
// 1. Sends heartbeat to group coordinator
// 2. Fetches messages from broker
// 3. Triggers rebalance callbacks if assignment changed

consumer.poll(Duration.ofMillis(100))
```

### max.poll.interval.ms trap

If `process(record)` takes longer than `max.poll.interval.ms` (default 5min), consumer is kicked out of group → rebalance → same message reassigned → infinite loop.

Fix: increase `max.poll.interval.ms`, or reduce `max.poll.records`, or process async.

### Offset commit strategies

|Strategy|How|Risk|
|---|---|---|
|Auto commit|Timer-based (every 5s)|Commits before processing completes → at-most-once|
|Manual sync|`commitSync()` — blocks|Safe, slow|
|Manual async|`commitAsync()` — non-blocking|Fast, no auto-retry on failure|
|Manual IMMEDIATE|Our setup — ack per record|Safest for record-level processing|

### Our consumer setup

```java
// AckMode.MANUAL_IMMEDIATE: offset committed the moment acknowledgment is called
factory.getContainerProperties().setAckMode(
    ContainerProperties.AckMode.MANUAL_IMMEDIATE
);

// In listener:
public void consume(ConsumerRecord<String, Object> record, Acknowledgment ack) {
    process(record);
    ack.acknowledge();  // commit this offset NOW
}
```

---

## 6. Delivery semantics

### At-most-once

- Commit offset **before** processing
- Message lost if crash after commit but before processing
- Never use for banking

### At-least-once (our setup)

- Commit offset **after** processing
- Message reprocessed if crash after processing but before commit
- Requires **idempotent consumers**

### Exactly-once (EOS)

- Kafka transactions wrap: read + process + write to output topic + commit offset
- Covers Kafka-to-Kafka only
- Writing to Postgres = outside transaction boundary → still at-least-once for DB writes
- Real solution = outbox pattern

### Idempotency guard (consumer side)

```java
// Check processed_events table before processing
if (processedEventRepository.existsById(eventId)) {
    acknowledgment.acknowledge();
    return; // skip duplicate
}

// After processing:
processedEventRepository.save(new ProcessedEvent(eventId, GROUP_ID, now()));
// If two instances race → DataIntegrityViolationException on PK → safe to ignore
```

---

## 7. Kafka Streams

### What it is

- Java library (not a separate cluster) inside your app process
- Runs as many instances as your app
- State stored in RocksDB locally, replicated via changelog topic to Kafka

### Core abstractions

|Abstraction|Description|
|---|---|
|`KStream`|Unbounded stream — each record independent|
|`KTable`|Changelog stream — latest value per key (upsert semantics)|
|`GlobalKTable`|Full KTable replicated to every instance — for lookup/reference data|

### KStream vs KTable join

- `KTable` join: partition 0 consumer can only join with keys on partition 0
- `GlobalKTable` join: every instance has all keys → use for small reference data (tenant configs, account tiers)

### State restoration

On crash/restart, Streams replays changelog topic to rebuild RocksDB. Can take minutes for large state.

Fix: `num.standby.replicas=1` → warm standby copy on another instance.

### Windowing types

```java
// Tumbling: fixed size, no overlap. Each event in exactly one window.
TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1))
// [10:00-11:00] [11:00-12:00] [12:00-13:00]

// Hopping: fixed size, overlapping. Event can be in multiple windows.
// size=1hr, advance=30min
// [10:00-11:00] [10:30-11:30] [11:00-12:00]

// Session: dynamic, gap-based. Events within gap=10min → same session.

// Sliding: all events within time distance of each other. Most expensive.
```

### Time types

|Type|Description|Use?|
|---|---|---|
|Event time|Timestamp in message|✅ Always|
|Ingestion time|When broker received it|Sometimes|
|Processing time|Wall clock when processed|❌ Never for windowing|

### Late events

```java
// Grace period: keep window open N minutes after end for late arrivals
TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(15))
// Events arriving 10:45 but processed at 11:10 still counted in 10:00-11:00 window
```

### Exactly-once in Streams

```java
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
// Atomically: read input + write output + write changelog + commit offset
// Still at-least-once for writes to external systems (Postgres)
```

### When NOT to use Kafka Streams

- Need DB lookup mid-stream → no built-in DB connector
- Huge state + slow restoration → use Flink with remote state backend
- Complex event patterns (CEP) → Flink CEP library
- Non-Java team → Streams is Java only

---

## 8. Outbox pattern

### Problem it solves

```java
// BROKEN — two separate systems, not atomic
ticketRepository.save(ticket);           // succeeds
kafkaProducer.send("topic", event);      // crashes → event lost forever
// OR
ticketRepository.save(ticket);           // crashes after write
kafkaProducer.send("topic", event);      // publishes for non-existent ticket
```

> `@Transactional` doesn't help — JpaTransactionManager only covers Postgres. Kafka is outside the transaction boundary entirely.

### Solution

```
Single DB transaction:
  INSERT INTO transactions (...)
  INSERT INTO outbox (topic, partition_key, payload, published=false)
  ← both or neither

Separate OutboxPoller (every 1s):
  SELECT * FROM outbox WHERE published=false FOR UPDATE SKIP LOCKED
  kafkaTemplate.send(...).get()     ← synchronous, wait for ack
  UPDATE outbox SET published=true
```

### FOR UPDATE SKIP LOCKED

```sql
SELECT * FROM outbox
WHERE published = false
ORDER BY created_at
LIMIT 100
FOR UPDATE SKIP LOCKED
-- If another instance locked a row, skip it
-- Prevents duplicate publishing with multiple app instances
-- No blocking, no deadlocks
```

### Outbox table schema

```sql
CREATE TABLE outbox (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic         VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255),
    payload       JSONB NOT NULL,              -- stored as JSONB
    published     BOOLEAN NOT NULL DEFAULT false,
    published_at  TIMESTAMP,
    retry_count   INT NOT NULL DEFAULT 0,
    last_error    TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT now()
);

-- Partial index — only unpublished rows. Stays small as table grows.
CREATE INDEX idx_outbox_unpublished ON outbox(created_at)
    WHERE published = false;
```

### Hibernate + JSONB gotcha

```java
// Hibernate sends String as varchar by default → Postgres rejects for jsonb column
// Fix: tell Hibernate to bind as JSON type
@Column(nullable = false, columnDefinition = "jsonb")
@org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
private String payload;
```

### Outbox table maintenance

```sql
-- Run nightly
DELETE FROM outbox
WHERE published = true
AND published_at < now() - INTERVAL '7 days';

-- Alert on these — events stuck after max retries
SELECT * FROM outbox
WHERE published = false AND retry_count > 5;
```

### Outbox vs direct publish comparison

||Direct publish|Outbox pattern|
|---|---|---|
|Atomicity|❌ No|✅ Yes (single DB tx)|
|Kafka down|Event lost|Event retried when Kafka recovers|
|App crash mid-send|Event lost|Event retried on restart|
|Duplicate risk|Low|At-least-once (consumer needs idempotency)|
|Latency|~0ms|~1s (poll interval)|

---

## 9. Dead letter queue

### Flow

```
transaction.initiated consumer
  → process() fails
  → retry 1 (backoff 1s)
  → retry 2 (backoff 2s)  
  → retry 3 (backoff 4s)
  → all retries exhausted
  → dlqProducer.send("transaction.dlq", key, payload)
  → acknowledgment.acknowledge()   ← must ack original, else stuck forever
```

### Why ack after DLQ routing

If you don't ack the original message after routing to DLQ:

```
offset 5 → fails → DLQ → no ack → restart → offset 5 again → DLQ loop
```

Consumer never moves past offset 5. Everything after it is blocked forever.

### Exponential backoff

```java
// attempt 1 → wait 1s  (2^0 * 1000)
// attempt 2 → wait 2s  (2^1 * 1000)
// attempt 3 → wait 4s  (2^2 * 1000)
long backoffMs = (long) Math.pow(2, attempt - 1) * 1000;
Thread.sleep(backoffMs);
```

### Poison pill handling

```java
// Message of unexpected type → route to DLQ immediately, don't retry
if (!(value instanceof Map)) {
    dlqProducer.sendToDlq(key, value, "Unexpected message type");
    acknowledgment.acknowledge();
    return;
}
```

---

## 10. BankStream project structure

```
bankstream/
├── docker-compose.yml
├── init.sql
├── pom.xml                          ← parent pom (groupId: com.bankstream)
│
├── transaction-service/             ← port 8090
│   ├── pom.xml
│   └── src/main/java/com/bankstream/transaction/
│       ├── TransactionServiceApplication.java
│       ├── config/
│       │   └── KafkaProducerConfig.java     ← objectKafkaTemplate + stringKafkaTemplate
│       ├── controller/
│       │   └── TransactionController.java   ← POST /api/transactions
│       ├── domain/
│       │   ├── Transaction.java             ← @Entity, transactions table
│       │   ├── TransactionStatus.java       ← INITIATED, COMPLETED, FAILED
│       │   ├── TransactionType.java         ← DEBIT, CREDIT
│       │   └── Outbox.java                  ← @Entity, outbox table
│       ├── event/
│       │   └── TransactionInitiatedEvent.java  ← Kafka message payload
│       ├── outbox/
│       │   └── OutboxPoller.java            ← @Scheduled, FOR UPDATE SKIP LOCKED
│       ├── producer/
│       │   └── TransactionProducer.java     ← publishes to transaction.initiated
│       ├── repository/
│       │   ├── TransactionRepository.java
│       │   └── OutboxRepository.java        ← findUnpublishedWithLock()
│       └── service/
│           └── TransactionService.java      ← @Transactional, saves tx + outbox entry
│
└── notification-service/            ← port 8091
    ├── pom.xml
    └── src/main/java/com/bankstream/notification/
        ├── NotificationServiceApplication.java
        ├── config/
        │   ├── KafkaConsumerConfig.java     ← MANUAL_IMMEDIATE, concurrency=3
        │   └── KafkaProducerConfig.java     ← for DLQ publishing
        ├── consumer/
        │   └── TransactionConsumer.java     ← @KafkaListener, retry + DLQ
        ├── dlq/
        │   └── DlqProducer.java             ← publishes to transaction.dlq
        ├── domain/
        │   └── ProcessedEvent.java          ← @Entity, idempotency guard
        ├── repository/
        │   └── ProcessedEventRepository.java
        └── service/
            └── NotificationService.java     ← simulates notification send
```

### Topics

|Topic|Partitions|Replication|Key|Purpose|
|---|---|---|---|---|
|`account.created`|3|2|account_id|Account creation events|
|`transaction.initiated`|3|2|account_id|New transactions|
|`transaction.completed`|3|2|account_id|Completed transactions|
|`transaction.failed`|3|2|account_id|Failed transactions|
|`fraud.alert`|3|2|account_id|Fraud detection results|
|`transaction.dlq`|1|2|—|Dead letter queue|

### Database tables

|Table|Owner|Purpose|
|---|---|---|
|`accounts`|seed data|Account master data|
|`transactions`|transaction-service|Business records|
|`outbox`|transaction-service|Guaranteed Kafka delivery|
|`processed_events`|notification-service|Consumer idempotency|
|`fraud_alerts`|fraud-detection-service (Phase 6)|Fraud records|

---

## 11. Docker Compose reference

### Services

|Container|Image|Port|Purpose|
|---|---|---|---|
|`bankstream-kafka1`|cp-kafka:7.6.0|9092/29092|Controller + Broker|
|`bankstream-kafka2`|cp-kafka:7.6.0|9094/29094|Broker only|
|`bankstream-schema-registry`|cp-schema-registry:7.6.0|8081|Avro schema store|
|`bankstream-kafka-ui`|provectuslabs/kafka-ui|8080|Web UI|
|`bankstream-postgres`|postgres:16-alpine|5432|Database|

### Useful commands

```bash
# Start everything
docker compose up -d

# Wipe everything including volumes (fresh start)
docker compose down -v

# Check status
docker compose ps

# View logs for a service
docker logs bankstream-kafka1

# Describe all topics
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 --describe

# Create a topic manually
docker exec -it bankstream-kafka1 kafka-topics \
  --bootstrap-server kafka1:9092 \
  --create --topic my.topic \
  --partitions 3 --replication-factor 2

# Trigger preferred leader election for a partition
docker exec -it bankstream-kafka1 kafka-leader-election \
  --bootstrap-server kafka1:9092 \
  --election-type preferred \
  --topic transaction.initiated --partition 1

# Check consumer group lag
docker exec -it bankstream-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --describe --group notification-service
```

### Why two ports per broker?

```
kafka1:
  9092  → internal Docker network (other containers use this)
  29092 → host machine (your Spring Boot app, tools on Windows)

kafka2:
  9094  → internal Docker network
  29094 → host machine
```

---

## 12. Config reference

### Producer configs (transaction-service application.yml)

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:29092,localhost:29094
    producer:
      acks: all                     # wait for all ISR — overridden by KafkaProducerConfig bean
      retries: 3
      enable-idempotence: true
      batch-size: 32768             # 32KB
      properties:
        linger.ms: 5                # wait 5ms to fill batch
        max.in.flight.requests.per.connection: 5  # must be ≤5 for idempotence
```

### Consumer configs (notification-service application.yml)

```yaml
spring:
  kafka:
    consumer:
      group-id: notification-service
      auto-offset-reset: earliest   # on first run, start from beginning
      enable-auto-commit: false     # we commit manually
      properties:
        spring.json.trusted.packages: "*"
```

### application.yml vs @Bean priority

`@Configuration` bean always wins over `application.yml`. Spring backs off auto-config when you define a bean manually. If you define the bean but forget a setting, it defaults (e.g. `acks` defaults to `1`, not `all`).

---

## 13. Debugging checklist

### Read stack traces bottom-up

```
Always scroll to the LAST "Caused by:" — that is the root cause.
Everything above it is Spring/Kafka wrapping the real error.
```

### Common errors and fixes

|Error|Cause|Fix|
|---|---|---|
|`Could not instantiate class com.fasterxml.jackson.databind.JsonSerializer`|Wrong import — Jackson's abstract class instead of Spring Kafka's|Change import to `org.springframework.kafka.support.serializer.JsonSerializer`|
|`column "payload" is of type jsonb but expression is of type character varying`|Hibernate sends String as varchar|Add `@JdbcTypeCode(SqlTypes.JSON)` to payload field|
|`Illegal attempt to set lock mode for a native query`|`@Lock` annotation on native query|Remove `@Lock` — `FOR UPDATE SKIP LOCKED` in SQL is enough|
|`required a bean of type KafkaTemplate that could not be found`|Two beans of same raw type, Spring can't choose|Add `@Bean("name")` and `@Qualifier("name")`|
|`constructor X is already defined`|`@RequiredArgsConstructor` + manual constructor conflict|Remove `@RequiredArgsConstructor` when writing constructor manually|
|`UnknownHostException: kafka2`|kafka1 tries to reach kafka2 before it starts (2-node Raft)|Make kafka2 broker-only, kafka1 sole controller|

### Check import when class name exists in multiple packages

Common conflicts:

- `JsonSerializer` → Jackson vs Spring Kafka
- `JsonDeserializer` → Jackson vs Spring Kafka
- `Message` → Spring Messaging vs Kafka clients

### Consumer lag — first health signal

```bash
docker exec -it bankstream-kafka1 kafka-consumer-groups \
  --bootstrap-server kafka1:9092 \
  --describe --group notification-service

# LAG column growing = consumers can't keep up
# LAG = 0 = healthy
```

---

## Phases remaining

- [x] Phase 1 — Docker Compose + infrastructure
- [x] Phase 2 — Raw producer/consumer (transaction-service)
- [x] Phase 3 — Outbox pattern
- [x] Phase 4 — DLQ + retry logic (notification-service)
- [ ] Phase 5 — Avro + Schema Registry
- [ ] Phase 6 — Kafka Streams (fraud-detection-service)