package io.ledgerflow.api.domain;

import io.ledgerflow.contracts.TransactionStatus;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transaction_status_history")
public class TransactionStatusHistoryEntity {
    @Id
    private UUID id;
    @Column(name = "transaction_id", nullable = false)
    private UUID transactionId;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TransactionStatus status;
    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;
    @Column(length = 200)
    private String reason;
    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    protected TransactionStatusHistoryEntity() {}

    public TransactionStatusHistoryEntity(UUID id, UUID transactionId, TransactionStatus status,
                                          UUID eventId, String reason, Instant occurredAt) {
        this.id = id;
        this.transactionId = transactionId;
        this.status = status;
        this.eventId = eventId;
        this.reason = reason;
        this.occurredAt = occurredAt;
    }

    public TransactionStatus getStatus() { return status; }
    public String getReason() { return reason; }
    public Instant getOccurredAt() { return occurredAt; }
}

