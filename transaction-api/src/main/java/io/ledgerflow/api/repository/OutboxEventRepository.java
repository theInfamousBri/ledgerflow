package io.ledgerflow.api.repository;

import io.ledgerflow.api.domain.OutboxEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {
    List<OutboxEventEntity> findByPublishedAtIsNullOrderByCreatedAtAsc(Pageable pageable);
}
