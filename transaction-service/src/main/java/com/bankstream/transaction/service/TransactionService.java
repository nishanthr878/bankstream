package com.bankstream.transaction.service;

import com.bankstream.transaction.domain.Transaction;
import com.bankstream.transaction.domain.TransactionStatus;
import com.bankstream.transaction.domain.TransactionType;
import com.bankstream.transaction.event.TransactionInitiatedEvent;
import com.bankstream.transaction.producer.TransactionProducer;
import com.bankstream.transaction.repository.TransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final TransactionProducer transactionProducer;

    @Transactional
    public Transaction initiateTransaction(UUID accountId,
                                           java.math.BigDecimal amount,
                                           TransactionType type,
                                           String description) {
        // Step 1: persist to DB
        Transaction transaction = Transaction.builder()
                .accountId(accountId)
                .amount(amount)
                .type(type)
                .status(TransactionStatus.INITIATED)
                .description(description)
                .build();

        transaction = transactionRepository.save(transaction);
        log.debug("Saved transaction {} for account {}", transaction.getId(), accountId);

        // Step 2: publish to Kafka
        // NOTE: this is Phase 2 — direct publish, no outbox yet
        // Problem: if Kafka is down, DB committed but event never published
        // We fix this in Phase 3 with the outbox pattern
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

        transactionProducer.publishTransactionInitiated(event);

        return transaction;
    }
}