package com.bankstream.notification.consumer;

import com.bankstream.notification.dlq.DlqProducer;
import com.bankstream.notification.domain.ProcessedEvent;
import com.bankstream.notification.repository.ProcessedEventRepository;
import com.bankstream.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionConsumer {

    private static final String GROUP_ID = "notification-service";
    private static final int MAX_RETRIES = 3;

    private final NotificationService notificationService;
    private final ProcessedEventRepository processedEventRepository;
    private final DlqProducer dlqProducer;

    @KafkaListener(
            topics = "transaction.initiated",
            groupId = GROUP_ID,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        String key = record.key();
        Object value = record.value();
        int partition = record.partition();
        long offset = record.offset();

        log.debug("Received message key = {} partition = {} offset={}", key, partition, offset);

        // Value comes in as LinkedHashMap when deserialized from JSON without type info
        if (!(value instanceof Map)) {
            log.error("Unexpected message type: {}, routing to DLQ", value.getClass());
            dlqProducer.sendToDlq(key, value, "Unexpected message type");
            acknowledgment.acknowledge(); // ack to move past this poison pill
            return;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> event = (Map<String, Object>) value;

        // Extract eventId for idempotency check
        String eventIdStr = (String) event.get("eventId");
        if (eventIdStr == null) {
            log.error("Message missing eventId, routing to DLQ");
            dlqProducer.sendToDlq(key, value, "Missing eventId");
            acknowledgment.acknowledge();
            return;
        }

        UUID eventId = UUID.fromString(eventIdStr);

        // Idempotency check — have we already processed this event?
        // processedEventRepository.existsById uses the primary key lookup
        // If yes: skip processing, still ack to move the offset forward
        if (processedEventRepository.existsById(eventId)) {
            log.info("Duplicate event {} detected, skipping", eventId);
            acknowledgment.acknowledge();
            return;
        }

        // Retry loop with exponential backoff
        // We retry inside the consumer before giving up and routing to DLQ
        // This handles transient failures (network blip, downstream timeout)
        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Attempt to send notification
                notificationService.sendTransactionNotification(event);

                // Success — mark as processed in DB
                // If this fails (DB down), we don't ack → message redelivered → idempotent
                try {
                    processedEventRepository.save(
                            ProcessedEvent.builder()
                                    .eventId(eventId)
                                    .consumerGroup(GROUP_ID)
                                    .processedAt(Instant.now())
                                    .build()
                    );
                } catch (DataIntegrityViolationException e) {
                    // Another instance already processed this event simultaneously
                    // Primary key violation — safe to ignore, idempotency holds
                    log.info("Race condition: event {} already processed by another instance", eventId);
                }

                // Commit offset — only after successful processing AND DB write
                acknowledgment.acknowledge();
                log.debug("Successfully processed event {} partition = {} offset={}",
                        eventId, partition, offset);
                return;
            } catch (Exception e) {
                lastException = e;
                log.warn("Attempt {}/{} failed for event {}: {}",
                        attempt, MAX_RETRIES, eventId, e.getMessage());

                if (attempt < MAX_RETRIES) {
                    try {
                        // Exponential backoff: 1s, 2s, 4s
                        // Gives downstream time to recover before next attempt
                        long backoofMs = (long) Math.pow(2, attempt - 1) * 1000;
                        log.debug("Backing off {}ms before retry", backoofMs);
                        Thread.sleep(backoofMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        // All retries exhausted — route to DLQ
        // Still ack the original message so consumer moves forward
        // The DLQ holds the message for manual inspection/replay
        log.error("All {} retries exhausted for event {}, routing to DLQ",
                MAX_RETRIES, eventId);
        dlqProducer.sendToDlq(key, value, lastException.getMessage());
        acknowledgment.acknowledge();

    }

}
