package com.bankstream.transaction.repository;

import com.bankstream.transaction.domain.Outbox;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxRepository extends JpaRepository<Outbox, UUID> {

    // FOR UPDATE SKIP LOCKED is in the SQL itself — no @Lock annotation needed
    // @Lock only works with JPQL, not native queries
    @Query(value = """
        SELECT * FROM outbox
        WHERE published = false
        ORDER BY created_at
        LIMIT 100
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<Outbox> findUnpublishedWithLock();
}