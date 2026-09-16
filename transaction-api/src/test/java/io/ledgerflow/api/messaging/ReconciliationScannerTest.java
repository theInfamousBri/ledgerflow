package io.ledgerflow.api.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.api.domain.OutboxEventEntity;
import io.ledgerflow.api.domain.TransactionEntity;
import io.ledgerflow.api.repository.OutboxEventRepository;
import io.ledgerflow.api.repository.TransactionRepository;
import io.ledgerflow.api.observability.TraceContextBridge;
import io.ledgerflow.api.observability.TraceContextBridge.PersistedTraceContext;
import io.ledgerflow.contracts.TransactionStatus;
import io.ledgerflow.contracts.TransactionType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReconciliationScannerTest {

    @Test
    void claimsStaleTransactionAndWritesRequestToOutbox() throws Exception {
        var transactions = mock(TransactionRepository.class);
        var outbox = mock(OutboxEventRepository.class);
        var registry = new SimpleMeterRegistry();
        var objectMapper = new ObjectMapper().findAndRegisterModules();
        var traces = mock(TraceContextBridge.class);
        when(traces.capture()).thenReturn(new PersistedTraceContext(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "vendor=value"));
        Instant now = Instant.parse("2026-09-13T12:00:00Z");
        UUID transactionId = UUID.randomUUID();
        var transaction = TransactionEntity.pending(
                transactionId,
                "reconciliation-test",
                "fingerprint",
                "acct-reconciliation",
                new BigDecimal("42.50"),
                "USD",
                TransactionType.PAYMENT,
                now.minus(Duration.ofMinutes(10)));
        transaction.transitionTo(TransactionStatus.PROCESSING, null, null, now.minus(Duration.ofMinutes(9)));
        when(transactions.lockReconciliationCandidates(
                now.minus(Duration.ofMinutes(2)),
                now.minus(Duration.ofMinutes(5)),
                25)).thenReturn(List.of(transaction));
        var scanner = new ReconciliationScanner(
                transactions,
                outbox,
                objectMapper,
                Clock.fixed(now, ZoneOffset.UTC),
                registry,
                traces,
                Duration.ofMinutes(2),
                Duration.ofMinutes(5),
                25);

        int queued = scanner.scan();

        assertThat(queued).isEqualTo(1);
        assertThat(transaction.getReconciliationRequestedAt()).isEqualTo(now);
        assertThat(transaction.getReconciliationAttempts()).isEqualTo(1);
        var eventCaptor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(outbox).save(eventCaptor.capture());
        assertThat(eventCaptor.getValue().getAggregateId()).isEqualTo(transactionId);
        assertThat(eventCaptor.getValue().getTopic()).isEqualTo(ReconciliationScanner.RECONCILIATION_TOPIC);
        assertThat(eventCaptor.getValue().getTraceParent())
                .isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        assertThat(eventCaptor.getValue().getTraceState()).isEqualTo("vendor=value");
        assertThat(objectMapper.readTree(eventCaptor.getValue().getPayload()).path("transactionId").asText())
                .isEqualTo(transactionId.toString());
        assertThat(registry.get("ledgerflow.reconciliation.scan").counter().count()).isEqualTo(1);
        assertThat(registry.get("ledgerflow.reconciliation.requested").counter().count()).isEqualTo(1);
    }
}
