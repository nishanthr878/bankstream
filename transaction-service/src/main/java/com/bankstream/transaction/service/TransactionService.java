package com.bankstream.transaction.service;

import com.bankstream.transaction.domain.Outbox;
import com.bankstream.transaction.domain.Transaction;
import com.bankstream.transaction.domain.TransactionStatus;
import com.bankstream.transaction.domain.TransactionType;
import com.bankstream.transaction.event.avro.TransactionInitiatedEvent;
import com.bankstream.transaction.producer.TransactionProducer;
import com.bankstream.transaction.repository.OutboxRepository;
import com.bankstream.transaction.repository.TransactionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.UUID;

@Slf4j
@Service
public class TransactionService {

    private final TransactionRepository transactionRepository;
    private final OutboxRepository outboxRepository;
    private final TransactionProducer transactionProducer;
    private final KafkaAvroSerializer kafkaAvroSerializer;

    public TransactionService(TransactionRepository transactionRepository,
                              OutboxRepository outboxRepository,
                              TransactionProducer transactionProducer,
                              KafkaAvroSerializer kafkaAvroSerializer) {
        this.transactionRepository = transactionRepository;
        this.outboxRepository = outboxRepository;
        this.transactionProducer = transactionProducer;
        this.kafkaAvroSerializer = kafkaAvroSerializer;
    }

    @Transactional
    public Transaction initiateTransaction(UUID accountId,
                                           BigDecimal amount,
                                           TransactionType type,
                                           String description) {
        // Step 1: save business record
        Transaction transaction = Transaction.builder()
                .accountId(accountId)
                .amount(amount)
                .type(type)
                .status(TransactionStatus.INITIATED)
                .description(description)
                .build();

        transaction = transactionRepository.save(transaction);
        log.debug("Saved transaction {} for account {}", transaction.getId(), accountId);

        String eventId = UUID.randomUUID().toString();

        // Step 2: build Avro event for direct Kafka publish
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

        byte[] avroBytes = kafkaAvroSerializer.serialize(
                "transaction.initiated",
                avroEvent
        );

        String base64Payload = Base64.getEncoder().encodeToString(avroBytes);

//
//        try {
//            Map<String, Object> outboxPayload = new LinkedHashMap<>();
//            outboxPayload.put("eventId", eventId);
//            outboxPayload.put("transactionId", transaction.getId().toString());
//            outboxPayload.put("accountId", transaction.getAccountId().toString());
//            outboxPayload.put("amount", transaction.getAmount().doubleValue());
//            outboxPayload.put("currency", transaction.getCurrency());
//            outboxPayload.put("type", transaction.getType().name());
//            outboxPayload.put("description", transaction.getDescription());
//            outboxPayload.put("occurredAt", transaction.getCreatedAt().toEpochMilli());
//
//            Outbox outboxEntry = Outbox.builder()
//                    .topic("transaction.initiated")
//                    .partitionKey(accountId.toString())
//                    .payload(objectMapper.writeValueAsString(outboxPayload))
//                    .build();
//
//            outboxRepository.save(outboxEntry);
//            log.debug("Wrote outbox entry for transaction {}", transaction.getId());
//        } catch (Exception e) {
//            throw new RuntimeException("Failed to serialize outbox payload", e);
//        }

        Outbox outboxEntry = Outbox.builder()
                .topic("transaction.initiated")
                .partitionKey(accountId.toString())
                .payload("\"" + base64Payload + "\"")
                .build();

        outboxRepository.save(outboxEntry);

        log.debug("Wrote outbox entry for transaction {}", transaction.getId());

        // Step 4: publish directly via Avro producer
        transactionProducer.publishTransactionInitiated(avroEvent);

        return transaction;
    }
}