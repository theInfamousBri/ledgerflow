package io.ledgerflow.api.domain;

import io.ledgerflow.contracts.TransactionStatus;
import io.ledgerflow.contracts.TransactionType;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "transactions")
public class TransactionEntity {
    @Id
    private UUID id;
    @Column(name = "idempotency_key", nullable = false, unique = true, length = 100)
    private String idempotencyKey;
    @Column(name = "request_fingerprint", nullable = false, length = 64)
    private String requestFingerprint;
    @Column(name = "account_id", nullable = false, length = 100)
    private String accountId;
    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;
    @Column(nullable = false, length = 3)
    private String currency;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TransactionType type;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TransactionStatus status;
    @Column(name = "provider_reference", length = 100)
    private String providerReference;
    @Column(name = "failure_code", length = 100)
    private String failureCode;
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
    @Version
    private long version;

    protected TransactionEntity() {}

    public static TransactionEntity pending(UUID id, String key, String fingerprint, String accountId,
                                            BigDecimal amount, String currency, TransactionType type, Instant now) {
        var transaction = new TransactionEntity();
        transaction.id = id;
        transaction.idempotencyKey = key;
        transaction.requestFingerprint = fingerprint;
        transaction.accountId = accountId;
        transaction.amount = amount;
        transaction.currency = currency;
        transaction.type = type;
        transaction.status = TransactionStatus.PENDING;
        transaction.createdAt = now;
        transaction.updatedAt = now;
        return transaction;
    }

    public void transitionTo(TransactionStatus next, String providerReference, String failureCode, Instant at) {
        this.status = next;
        this.providerReference = providerReference;
        this.failureCode = failureCode;
        this.updatedAt = at;
    }

    public UUID getId() { return id; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public String getRequestFingerprint() { return requestFingerprint; }
    public String getAccountId() { return accountId; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public TransactionType getType() { return type; }
    public TransactionStatus getStatus() { return status; }
    public String getProviderReference() { return providerReference; }
    public String getFailureCode() { return failureCode; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}

