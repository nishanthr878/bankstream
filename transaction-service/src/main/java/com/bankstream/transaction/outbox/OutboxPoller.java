package com.bankstream.transaction.outbox;

import com.bankstream.transaction.domain.Outbox;
import com.bankstream.transaction.repository.OutboxRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

@Slf4j
@Component

public class OutboxPoller {

    private final OutboxRepository outboxRepository;
    // Use String template - payload is already serialized JSON string
    // Using Object template would double-serialize it
    private final KafkaTemplate<String, String> stringKafkaTemplate;

    public OutboxPoller(
            OutboxRepository outboxRepository,
            @Qualifier("stringKafkaTemplate") KafkaTemplate<String, String> stringKafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.stringKafkaTemplate = stringKafkaTemplate;
    }

    private static final int MAX_RETRY = 5;

    // Runs every 1000ms
    // @Transactional here is critical — the SELECT FOR UPDATE SKIP LOCKED
    // lock is held for the duration of the transaction
    // Without it, the lock releases immediately after the query
    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void poll() {
        List<Outbox> unpublished = outboxRepository.findUnpublishedWithLock();

        if(unpublished.isEmpty()) return;

        log.debug("Found {} unpublished outbox entries", unpublished.size());

        for (Outbox entry : unpublished) {
            try {
                // Synchronous send — we need confirmation before marking published
                // If Kafka is down this throws, we catch below and increment retry
                stringKafkaTemplate.send(
                        entry.getTopic(),
                        entry.getPartitionKey(),
                        entry.getPayload()
                ).get(); // blocks until broker acks

                // Mark published — same DB transaction
                entry.setPublished(true);
                entry.setPublishedAt(Instant.now());
                outboxRepository.save(entry);

                log.debug("Published outbox entry {} to topic {}",
                        entry.getId(), entry.getTopic());

            } catch (Exception e) {
                // Kafka unavailable or timeout
                entry.setRetryCount(entry.getRetryCount() + 1);
                entry.setLastError(e.getMessage());
                outboxRepository.save(entry);

                if (entry.getRetryCount() >= MAX_RETRY) {
                    log.error("Outbox entry {} failed {} times, needs manual intervention",
                            entry.getId(), entry.getRetryCount());
                } else {
                    log.warn("Failed to publish outbox entry {}, retry {}/{}",
                            entry.getId(), entry.getRetryCount(), MAX_RETRY);
                }
            }
        }

    }
}
