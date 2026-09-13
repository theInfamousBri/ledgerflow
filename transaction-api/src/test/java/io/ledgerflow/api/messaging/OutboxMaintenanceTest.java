package io.ledgerflow.api.messaging;

import io.ledgerflow.api.repository.OutboxEventRepository;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxMaintenanceTest {

    @Test
    void deletesOneBoundedBatchOlderThanRetentionAndRecordsCount() {
        var repository = mock(OutboxEventRepository.class);
        var metrics = mock(OutboxMetrics.class);
        Instant now = Instant.parse("2026-09-13T12:00:00Z");
        var maintenance = new OutboxMaintenance(
                repository,
                metrics,
                Clock.fixed(now, ZoneOffset.UTC),
                Duration.ofDays(7),
                250);
        Instant expectedCutoff = Instant.parse("2026-09-06T12:00:00Z");
        when(repository.deletePublishedBefore(expectedCutoff, 250)).thenReturn(37);

        int deleted = maintenance.cleanupPublishedEvents();

        assertThat(deleted).isEqualTo(37);
        verify(repository).deletePublishedBefore(expectedCutoff, 250);
        verify(metrics).recordCleanup(37);
    }
}
