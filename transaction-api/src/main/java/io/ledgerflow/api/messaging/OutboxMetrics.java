package io.ledgerflow.api.messaging;

import io.ledgerflow.api.repository.OutboxEventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class OutboxMetrics {
    private static final Logger log = LoggerFactory.getLogger(OutboxMetrics.class);

    private final OutboxEventRepository repository;
    private final Clock clock;
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong oldestPendingAgeSeconds = new AtomicLong();
    private final Counter published;
    private final Counter publishFailures;
    private final Counter cleanupDeleted;
    private final Counter refreshFailures;

    public OutboxMetrics(OutboxEventRepository repository, MeterRegistry registry, Clock clock) {
        this.repository = repository;
        this.clock = clock;
        this.published = Counter.builder("ledgerflow.outbox.publish.success")
                .description("Outbox events successfully published to Kafka")
                .register(registry);
        this.publishFailures = Counter.builder("ledgerflow.outbox.publish.failure")
                .description("Outbox publication attempts that failed")
                .register(registry);
        this.cleanupDeleted = Counter.builder("ledgerflow.outbox.cleanup.deleted")
                .description("Published outbox events removed by retention cleanup")
                .register(registry);
        this.refreshFailures = Counter.builder("ledgerflow.outbox.metrics.refresh.failure")
                .description("Outbox metric snapshot refreshes that failed")
                .register(registry);

        Gauge.builder("ledgerflow.outbox.pending", pending, AtomicLong::get)
                .description("Number of outbox events waiting to be published")
                .register(registry);
        Gauge.builder("ledgerflow.outbox.oldest.pending.age", oldestPendingAgeSeconds, AtomicLong::get)
                .description("Age in seconds of the oldest unpublished outbox event")
                .baseUnit("seconds")
                .register(registry);
    }

    @Scheduled(
            fixedDelayString = "${ledgerflow.outbox.metrics-refresh-delay:15s}",
            initialDelayString = "${ledgerflow.outbox.metrics-initial-delay:5s}")
    public void refresh() {
        try {
            long pendingCount = repository.countByPublishedAtIsNull();
            long oldestAge = repository.findOldestUnpublishedCreatedAt()
                    .map(this::ageSeconds)
                    .orElse(0L);
            pending.set(pendingCount);
            oldestPendingAgeSeconds.set(oldestAge);
        } catch (RuntimeException exception) {
            refreshFailures.increment();
            log.warn("Could not refresh outbox metrics", exception);
        }
    }

    void recordPublished() {
        published.increment();
    }

    void recordPublishFailure() {
        publishFailures.increment();
    }

    void recordCleanup(int deleted) {
        cleanupDeleted.increment(deleted);
    }

    private long ageSeconds(Instant createdAt) {
        return Math.max(0, Duration.between(createdAt, Instant.now(clock)).toSeconds());
    }
}
