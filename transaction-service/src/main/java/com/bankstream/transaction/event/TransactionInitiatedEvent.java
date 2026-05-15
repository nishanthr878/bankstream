package com.bankstream.transaction.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

// This is the payload published to Kafka topic: transaction.initiated
// Intentionally a separate class from Transaction domain entity
// Domain entity = DB shape, Event = Kafka message shape
// They evolve independently
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TransactionInitiatedEvent {

    // Used as idempotency key by consumers
    private UUID eventId;

    private UUID transactionId;
    private UUID accountId;
    private BigDecimal amount;
    private String currency;
    private String type;
    private String description;
    private Instant occurredAt;
}