package com.bankstream.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "processed_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedEvent {
    // eventId is the primary key — same UUID from the Kafka message
    // If we try to insert the same eventId twice, DB throws unique violation
    // That's our idempotency guard — DB enforces it, not application logic
    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "consumer_group", nullable = false)
    private String consumerGroup;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

}
