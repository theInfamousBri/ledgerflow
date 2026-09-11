package io.ledgerflow.api.repository;

import io.ledgerflow.api.domain.TransactionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
import java.util.UUID;

public interface TransactionRepository extends JpaRepository<TransactionEntity, UUID> {
    Optional<TransactionEntity> findByIdempotencyKey(String idempotencyKey);
}

