package com.bankstream.transaction.domain;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Outbox {
    @Id
    @GeneratedValue(strategy  = GenerationType.UUID)
    private UUID id;

    // Kafka topic this event should be published to
    @Column(nullable = false)
    private String topic;

    // Kafka partition key — account_id in our case
    @Column(name = "partition_key")
    private String partitionKey;


    // Full event payload serialized as JSON
    // Stored as JSONB in Postgres — queryable, indexable
    @Column(nullable = false, columnDefinition = "jsonb")
    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    private String payload;

    @Column(nullable = false)
    private boolean published;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "last_error")
    private String lastError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @PrePersist
    void prePersist() {
        createdAt = Instant.now();
        published = false;
        retryCount = 0;
    }
}
