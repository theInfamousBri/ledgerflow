package io.ledgerflow.api.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.api.domain.OutboxEventEntity;
import io.ledgerflow.api.repository.OutboxEventRepository;
import io.ledgerflow.api.repository.TransactionRepository;
import io.ledgerflow.contracts.TransactionReconciliationRequestedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
public class ReconciliationScanner {
    public static final String RECONCILIATION_TOPIC =
            "ledgerflow.transaction.reconciliation.requested.v1";

    private static final Logger log = LoggerFactory.getLogger(ReconciliationScanner.class);

    private final TransactionRepository transactions;
    private final OutboxEventRepository outbox;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Duration staleAfter;
    private final Duration retryDelay;
    private final int batchSize;
    private final Counter scans;
    private final Counter requested;

    public ReconciliationScanner(
            TransactionRepository transactions,
            OutboxEventRepository outbox,
            ObjectMapper objectMapper,
            Clock clock,
            MeterRegistry registry,
            @Value("${ledgerflow.reconciliation.stale-after:2m}") Duration staleAfter,
            @Value("${ledgerflow.reconciliation.retry-delay:5m}") Duration retryDelay,
            @Value("${ledgerflow.reconciliation.batch-size:100}") int batchSize) {
        if (staleAfter.isZero() || staleAfter.isNegative()) {
            throw new IllegalArgumentException("ledgerflow.reconciliation.stale-after must be positive");
        }
        if (retryDelay.isZero() || retryDelay.isNegative()) {
            throw new IllegalArgumentException("ledgerflow.reconciliation.retry-delay must be positive");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("ledgerflow.reconciliation.batch-size must be positive");
        }
        this.transactions = transactions;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
        this.clock = clock;
        this.staleAfter = staleAfter;
        this.retryDelay = retryDelay;
        this.batchSize = batchSize;
        this.scans = Counter.builder("ledgerflow.reconciliation.scan")
                .description("Completed stale-transaction reconciliation scans")
                .register(registry);
        this.requested = Counter.builder("ledgerflow.reconciliation.requested")
                .description("Stale transactions queued for provider reconciliation")
                .register(registry);
    }

    @Scheduled(
            fixedDelayString = "${ledgerflow.reconciliation.scan-delay:30s}",
            initialDelayString = "${ledgerflow.reconciliation.initial-delay:30s}")
    @Transactional
    public int scan() {
        Instant now = Instant.now(clock);
        var candidates = transactions.lockReconciliationCandidates(
                now.minus(staleAfter), now.minus(retryDelay), batchSize);

        for (var transaction : candidates) {
            UUID eventId = UUID.randomUUID();
            var event = new TransactionReconciliationRequestedEvent(
                    eventId,
                    1,
                    transaction.getId(),
                    "reconcile-" + eventId,
                    now);
            transaction.markReconciliationRequested(now);
            outbox.save(new OutboxEventEntity(
                    eventId,
                    transaction.getId(),
                    RECONCILIATION_TOPIC,
                    serialize(event),
                    now));
        }

        if (!candidates.isEmpty()) {
            requested.increment(candidates.size());
            log.info("Queued {} stale transactions for reconciliation", candidates.size());
        }
        scans.increment();
        return candidates.size();
    }

    private String serialize(TransactionReconciliationRequestedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize reconciliation event", exception);
        }
    }
}
