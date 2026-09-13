package io.ledgerflow.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.contracts.ProviderPaymentResponse;
import io.ledgerflow.contracts.ProviderPaymentStatus;
import io.ledgerflow.contracts.TransactionReconciliationRequestedEvent;
import io.ledgerflow.contracts.TransactionStatusChangedEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReconciliationRequestedListenerTest {

    @Test
    void emitsCompletedStatusForRecordedProviderDecision() throws Exception {
        var objectMapper = new ObjectMapper().findAndRegisterModules();
        var provider = mock(ProviderClient.class);
        @SuppressWarnings("unchecked")
        var kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        var registry = new SimpleMeterRegistry();
        UUID transactionId = UUID.randomUUID();
        var request = request(transactionId);
        var decision = new ProviderPaymentResponse(true, "sim-recorded", null);
        when(provider.findPayment(transactionId)).thenReturn(Optional.of(
                new ProviderPaymentStatus(transactionId, decision, 1, List.of(Instant.now()))));
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.<SendResult<String, String>>completedFuture(null));
        var listener = new ReconciliationRequestedListener(objectMapper, provider, kafka, registry);

        listener.onRequested(objectMapper.writeValueAsString(request));

        var payload = ArgumentCaptor.forClass(String.class);
        verify(kafka).send(
                org.mockito.ArgumentMatchers.eq("ledgerflow.transaction.status.v1"),
                org.mockito.ArgumentMatchers.eq(transactionId.toString()),
                payload.capture());
        var status = objectMapper.readValue(payload.getValue(), TransactionStatusChangedEvent.class);
        assertThat(status.status().name()).isEqualTo("COMPLETED");
        assertThat(status.providerReference()).isEqualTo("sim-recorded");
        assertThat(registry.get("ledgerflow.reconciliation.resolved").counter().count()).isEqualTo(1);
    }

    @Test
    void leavesTransactionUnchangedWhenProviderHasNoRecord() throws Exception {
        var objectMapper = new ObjectMapper().findAndRegisterModules();
        var provider = mock(ProviderClient.class);
        @SuppressWarnings("unchecked")
        var kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        var registry = new SimpleMeterRegistry();
        UUID transactionId = UUID.randomUUID();
        when(provider.findPayment(transactionId)).thenReturn(Optional.empty());
        var listener = new ReconciliationRequestedListener(objectMapper, provider, kafka, registry);

        listener.onRequested(objectMapper.writeValueAsString(request(transactionId)));

        verify(kafka, never()).send(anyString(), anyString(), anyString());
        assertThat(registry.get("ledgerflow.reconciliation.unresolved").counter().count()).isEqualTo(1);
    }

    private TransactionReconciliationRequestedEvent request(UUID transactionId) {
        return new TransactionReconciliationRequestedEvent(
                UUID.randomUUID(),
                1,
                transactionId,
                "reconciliation-test-trace",
                Instant.now());
    }
}
