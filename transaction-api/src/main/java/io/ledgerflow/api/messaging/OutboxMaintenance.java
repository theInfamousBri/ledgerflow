package io.ledgerflow.api.messaging;

import io.ledgerflow.api.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

@Component
public class OutboxMaintenance {
    private static final Logger log = LoggerFactory.getLogger(OutboxMaintenance.class);

    private final OutboxEventRepository repository;
    private final OutboxMetrics metrics;
    private final Clock clock;
    private final Duration retention;
    private final int cleanupBatchSize;

    public OutboxMaintenance(OutboxEventRepository repository,
                             OutboxMetrics metrics,
                             Clock clock,
                             @Value("${ledgerflow.outbox.retention:7d}") Duration retention,
                             @Value("${ledgerflow.outbox.cleanup-batch-size:1000}") int cleanupBatchSize) {
        if (retention.isZero() || retention.isNegative()) {
            throw new IllegalArgumentException("ledgerflow.outbox.retention must be positive");
        }
        if (cleanupBatchSize < 1) {
            throw new IllegalArgumentException("ledgerflow.outbox.cleanup-batch-size must be positive");
        }
        this.repository = repository;
        this.metrics = metrics;
        this.clock = clock;
        this.retention = retention;
        this.cleanupBatchSize = cleanupBatchSize;
    }

    @Scheduled(
            fixedDelayString = "${ledgerflow.outbox.cleanup-delay:1h}",
            initialDelayString = "${ledgerflow.outbox.cleanup-initial-delay:1m}")
    @Transactional
    public int cleanupPublishedEvents() {
        Instant cutoff = Instant.now(clock).minus(retention);
        int deleted = repository.deletePublishedBefore(cutoff, cleanupBatchSize);
        if (deleted > 0) {
            metrics.recordCleanup(deleted);
            log.info("Deleted {} published outbox events older than {}", deleted, cutoff);
        }
        return deleted;
    }
}
