package com.bankstream.transaction.producer;

import com.bankstream.transaction.event.TransactionInitiatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionProducer {

    private static final String TOPIC = "transaction.initiated";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishTransactionInitiated(TransactionInitiatedEvent event) {
        // Partition key = accountId
        // All transactions for the same account go to the same partition
        // Guarantees ordering per account
        String partitionKey = event.getAccountId().toString();

        CompletableFuture<SendResult<String, Object>> future =
                kafkaTemplate.send(TOPIC, partitionKey, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish event {} for account {}: {}",
                        event.getEventId(), event.getAccountId(), ex.getMessage());
            } else {
                log.debug("Published event {} to partition {} offset {}",
                        event.getEventId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}