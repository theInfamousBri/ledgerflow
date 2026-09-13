package io.ledgerflow.api.repository;

import io.ledgerflow.api.domain.OutboxEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {
    List<OutboxEventEntity> findByPublishedAtIsNullOrderByCreatedAtAsc(Pageable pageable);

    long countByPublishedAtIsNull();

    @Query("SELECT MIN(event.createdAt) FROM OutboxEventEntity event WHERE event.publishedAt IS NULL")
    Optional<Instant> findOldestUnpublishedCreatedAt();

    @Modifying
    @Query(value = """
            WITH cleanup_batch AS (
                SELECT id
                FROM outbox_events
                WHERE published_at IS NOT NULL
                  AND published_at < :cutoff
                ORDER BY published_at
                FOR UPDATE SKIP LOCKED
                LIMIT :batchSize
            )
            DELETE FROM outbox_events AS event
            USING cleanup_batch
            WHERE event.id = cleanup_batch.id
            """, nativeQuery = true)
    int deletePublishedBefore(@Param("cutoff") Instant cutoff, @Param("batchSize") int batchSize);
}
