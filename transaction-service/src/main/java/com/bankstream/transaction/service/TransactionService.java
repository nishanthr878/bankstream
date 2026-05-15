package com.bankstream.transaction.service;

import com.bankstream.transaction.domain.Outbox;
import com.bankstream.transaction.domain.Transaction;
import com.bankstream.transaction.domain.TransactionStatus;
import com.bankstream.transaction.domain.TransactionType;
import com.bankstream.transaction.event.TransactionInitiatedEvent;
import com.bankstream.transaction.repository.OutboxRepository;
import com.bankstream.transaction.repository.TransactionRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Transaction initiateTransaction(UUID accountId,
                                           BigDecimal amount,
                                           TransactionType type,
                                           String description) {
        // Step 1: persist transaction to DB
        Transaction transaction = Transaction.builder()
                .accountId(accountId)
                .amount(amount)
                .type(type)
                .status(TransactionStatus.INITIATED)
                .description(description)
                .build();

        transaction = transactionRepository.save(transaction);
        log.debug("Saved transaction {} for account {}", transaction.getId(), accountId);

        // Step 2: write outbox entry IN THE SAME DB TRANSACTION
        // If anything fails here, BOTH the transaction row and outbox row roll back
        // No phantom events, no lost events
        TransactionInitiatedEvent event = TransactionInitiatedEvent.builder()
                .eventId(UUID.randomUUID())
                .transactionId(transaction.getId())
                .accountId(transaction.getAccountId())
                .amount(transaction.getAmount())
                .currency(transaction.getCurrency())
                .type(transaction.getType().name())
                .description(transaction.getDescription())
                .occurredAt(transaction.getCreatedAt())
                .build();

        try {
            Outbox outboxEntry = Outbox.builder()
                    .topic("transaction.initiated")
                    .partitionKey(accountId.toString())
                    .payload(objectMapper.writeValueAsString(event))
                    .build();

            outboxRepository.save(outboxEntry);
            log.debug("Wrote outbox entry for transaction {}", transaction.getId());
        } catch (JsonProcessingException e) {
            // If serialization fails, whole transaction rolls back
            throw new RuntimeException("Failed to serialize event", e);
        }

        // No Kafka call here — the poller handles that separately
        return transaction;
    }
}