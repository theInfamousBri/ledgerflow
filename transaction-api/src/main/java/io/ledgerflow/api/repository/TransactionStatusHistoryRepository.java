package io.ledgerflow.api.repository;

import io.ledgerflow.api.domain.TransactionStatusHistoryEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface TransactionStatusHistoryRepository extends JpaRepository<TransactionStatusHistoryEntity, UUID> {
    List<TransactionStatusHistoryEntity> findByTransactionIdOrderByOccurredAtAsc(UUID transactionId);
    boolean existsByEventId(UUID eventId);
}

