package com.bankstream.notification.consumer;

import com.bankstream.transaction.event.avro.TransactionInitiatedEvent;
import com.bankstream.notification.dlq.DlqProducer;
import com.bankstream.notification.domain.ProcessedEvent;
import com.bankstream.notification.repository.ProcessedEventRepository;
import com.bankstream.notification.service.NotificationService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
public class TransactionConsumer {

    private static final String GROUP_ID = "notification-service";
    private static final int MAX_RETRIES = 3;

    private final NotificationService notificationService;
    private final ProcessedEventRepository processedEventRepository;
    private final DlqProducer dlqProducer;

    public TransactionConsumer(NotificationService notificationService,
                               ProcessedEventRepository processedEventRepository,
                               DlqProducer dlqProducer) {
        this.notificationService = notificationService;
        this.processedEventRepository = processedEventRepository;
        this.dlqProducer = dlqProducer;
    }

    @KafkaListener(
            topics = "transaction.initiated",
            groupId = GROUP_ID,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, Object> record,
                        Acknowledgment acknowledgment) {

        String key = record.key();
        Object value = record.value();
        int partition = record.partition();
        long offset = record.offset();

        log.debug("Received message key={} partition={} offset={}", key, partition, offset);

        // With Avro + specific.avro.reader=true, value is already a typed object
        if (!(value instanceof TransactionInitiatedEvent)) {
            log.error("Unexpected message type: {}, routing to DLQ", value.getClass());
            dlqProducer.sendToDlq(key, value, "Unexpected message type");
            acknowledgment.acknowledge();
            return;
        }

        TransactionInitiatedEvent event = (TransactionInitiatedEvent) value;

        // eventId is now a String directly — no map.get() needed
        UUID eventId = UUID.fromString(event.getEventId());

        // Idempotency check
        if (processedEventRepository.existsById(eventId)) {
            log.info("Duplicate event {} detected, skipping", eventId);
            acknowledgment.acknowledge();
            return;
        }

        // Retry loop with exponential backoff
        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                // Pass typed Avro object directly — no more map casting
                notificationService.sendTransactionNotification(event);

                try {
                    processedEventRepository.save(
                            ProcessedEvent.builder()
                                    .eventId(eventId)
                                    .consumerGroup(GROUP_ID)
                                    .processedAt(Instant.now())
                                    .build()
                    );
                } catch (DataIntegrityViolationException e) {
                    log.info("Race condition: event {} already processed", eventId);
                }

                acknowledgment.acknowledge();
                log.debug("Successfully processed event {} partition={} offset={}",
                        eventId, partition, offset);
                return;

            } catch (DataIntegrityViolationException e) {
                acknowledgment.acknowledge();
                return;
            } catch (Exception e) {
                lastException = e;
                log.warn("Attempt {}/{} failed for event {}: {}",
                        attempt, MAX_RETRIES, eventId, e.getMessage());

                if (attempt < MAX_RETRIES) {
                    try {
                        long backoffMs = (long) Math.pow(2, attempt - 1) * 1000;
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        log.error("All {} retries exhausted for event {}, routing to DLQ",
                MAX_RETRIES, eventId);
        dlqProducer.sendToDlq(key, value, lastException.getMessage());
        acknowledgment.acknowledge();
    }
}