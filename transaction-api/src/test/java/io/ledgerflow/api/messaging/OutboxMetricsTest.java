package io.ledgerflow.api.messaging;

import io.ledgerflow.api.repository.OutboxEventRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxMetricsTest {

    @Test
    void refreshesBacklogAndOldestPendingAgeWithoutDatabaseQueriesDuringScrape() {
        var repository = mock(OutboxEventRepository.class);
        var registry = new SimpleMeterRegistry();
        Instant now = Instant.parse("2026-09-13T12:00:00Z");
        Instant oldest = Instant.parse("2026-09-13T11:58:30Z");
        when(repository.countByPublishedAtIsNull()).thenReturn(3L);
        when(repository.findOldestUnpublishedCreatedAt())
                .thenReturn(Optional.of(oldest));
        var metrics = new OutboxMetrics(
                repository,
                registry,
                Clock.fixed(now, ZoneOffset.UTC));

        metrics.refresh();

        assertThat(registry.get("ledgerflow.outbox.pending").gauge().value()).isEqualTo(3);
        assertThat(registry.get("ledgerflow.outbox.oldest.pending.age").gauge().value()).isEqualTo(90);

        metrics.recordPublished();
        metrics.recordPublishFailure();
        metrics.recordCleanup(12);
        assertThat(registry.get("ledgerflow.outbox.publish.success").counter().count()).isEqualTo(1);
        assertThat(registry.get("ledgerflow.outbox.publish.failure").counter().count()).isEqualTo(1);
        assertThat(registry.get("ledgerflow.outbox.cleanup.deleted").counter().count()).isEqualTo(12);
    }
}
