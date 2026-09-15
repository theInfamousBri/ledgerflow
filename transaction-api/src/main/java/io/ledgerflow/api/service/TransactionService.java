package io.ledgerflow.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.api.domain.*;
import io.ledgerflow.api.repository.*;
import io.ledgerflow.api.web.CreateTransactionRequest;
import io.ledgerflow.api.web.TransactionResponse;
import io.ledgerflow.contracts.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

@Service
public class TransactionService {
    public static final String REQUESTED_TOPIC = "ledgerflow.transaction.requested.v1";

    private final TransactionRepository transactions;
    private final TransactionStatusHistoryRepository history;
    private final OutboxEventRepository outbox;
    private final ObjectMapper objectMapper;
    private final IdempotencyLock idempotencyLock;
    private final TransactionCache cache;

    public TransactionService(TransactionRepository transactions,
                              TransactionStatusHistoryRepository history,
                              OutboxEventRepository outbox,
                              ObjectMapper objectMapper,
                              IdempotencyLock idempotencyLock,
                              TransactionCache cache) {
        this.transactions = transactions;
        this.history = history;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.idempotencyLock = idempotencyLock;
        this.cache = cache;
    }

    @Transactional
    public CreationResult create(String idempotencyKey, CreateTransactionRequest request, String traceId) {
        String fingerprint = fingerprint(request);
        idempotencyLock.acquire(idempotencyKey);
        var existing = transactions.findByIdempotencyKey(idempotencyKey);
        if (existing.isPresent()) {
            if (!existing.get().getRequestFingerprint().equals(fingerprint)) {
                throw new IdempotencyConflictException();
            }
            return new CreationResult(toResponse(existing.get()), false);
        }

        Instant now = Instant.now();
        UUID transactionId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        var entity = TransactionEntity.pending(transactionId, idempotencyKey, fingerprint,
                request.accountId(), request.amount(), request.currency(), request.type(), now);
        transactions.save(entity);
        history.save(new TransactionStatusHistoryEntity(
                UUID.randomUUID(), transactionId, TransactionStatus.PENDING, eventId, "Request accepted", now));

        var event = new TransactionRequestedEvent(eventId, 1, transactionId, request.accountId(),
                request.amount(), request.currency(), request.type(), traceId, now);
        outbox.save(new OutboxEventEntity(eventId, transactionId, REQUESTED_TOPIC, serialize(event), now));
        return new CreationResult(toResponse(entity), true);
    }

    @Transactional(readOnly = true)
    public TransactionResponse get(UUID id) {
        var cached = cache.get(id);
        if (cached.isPresent()) return cached.get();

        var response = toResponse(transactions.findById(id)
                .orElseThrow(() -> new TransactionNotFoundException(id)));
        cache.put(response);
        return response;
    }

    @Transactional
    public void apply(TransactionStatusChangedEvent event) {
        cache.evictAfterCommit(event.transactionId());
        if (history.existsByEventId(event.eventId())) return;
        var entity = transactions.findById(event.transactionId())
                .orElseThrow(() -> new TransactionNotFoundException(event.transactionId()));
        if (entity.getStatus() == event.status()) return;
        if (TransactionStateMachine.isStale(entity.getStatus(), event.status())) return;
        if (!TransactionStateMachine.isAllowed(entity.getStatus(), event.status())) {
            throw new IllegalStateException("Illegal transaction transition %s -> %s"
                    .formatted(entity.getStatus(), event.status()));
        }
        entity.transitionTo(event.status(), event.providerReference(), event.failureCode(), event.occurredAt());
        history.save(new TransactionStatusHistoryEntity(UUID.randomUUID(), event.transactionId(), event.status(),
                event.eventId(), event.failureCode(), event.occurredAt()));
    }

    private TransactionResponse toResponse(TransactionEntity entity) {
        var items = history.findByTransactionIdOrderByOccurredAtAsc(entity.getId()).stream()
                .map(item -> new TransactionResponse.HistoryItem(item.getStatus(), item.getReason(), item.getOccurredAt()))
                .toList();
        return new TransactionResponse(entity.getId(), entity.getAccountId(), entity.getAmount(), entity.getCurrency(),
                entity.getType(), entity.getStatus(), entity.getProviderReference(), entity.getFailureCode(),
                entity.getCreatedAt(), entity.getUpdatedAt(), items);
    }

    private String fingerprint(CreateTransactionRequest request) {
        String canonical = String.join("|", request.accountId(), normalize(request.amount()),
                request.currency(), request.type().name());
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 unavailable", exception);
        }
    }

    private String normalize(BigDecimal amount) {
        return amount.stripTrailingZeros().toPlainString();
    }

    private String serialize(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize outbox event", exception);
        }
    }

    public record CreationResult(TransactionResponse transaction, boolean created) {}
}
