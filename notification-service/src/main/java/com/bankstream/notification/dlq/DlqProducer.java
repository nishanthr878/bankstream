package com.bankstream.notification.dlq;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DlqProducer {

    private static final String DLQ_TOPIC = "transaction.dlq";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void sendToDlq(String key, Object payload, String reason) {
        log.error("Routing message to DLQ. Key:{}, Reason:{}", key, reason);

        kafkaTemplate.send(DLQ_TOPIC, key, payload)
                .whenComplete((result, ex) -> {
                   if (ex != null) {
                       // DLQ publish failed — this is a critical alert
                       // In production: page on-call, write to DB, never silently drop
                       log.error("CRITICAL: Failed to publis to DLQ for key{}: {}",
                               key, ex.getMessage());
                   } else {
                       log.info("Message routed to DLQ at partition {} offset {}",
                               result.getRecordMetadata().partition(),
                               result.getRecordMetadata().offset());
                   }
                });
    }
}
