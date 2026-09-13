package io.ledgerflow.api.repository;

import io.ledgerflow.api.domain.TransactionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TransactionRepository extends JpaRepository<TransactionEntity, UUID> {
    Optional<TransactionEntity> findByIdempotencyKey(String idempotencyKey);

    @Query(value = """
            SELECT *
            FROM transactions
            WHERE status = 'PROCESSING'
              AND updated_at < :staleBefore
              AND (
                    reconciliation_requested_at IS NULL
                    OR reconciliation_requested_at < :retryBefore
                  )
            ORDER BY updated_at
            FOR UPDATE SKIP LOCKED
            LIMIT :batchSize
            """, nativeQuery = true)
    List<TransactionEntity> lockReconciliationCandidates(
            @Param("staleBefore") Instant staleBefore,
            @Param("retryBefore") Instant retryBefore,
            @Param("batchSize") int batchSize);
}

