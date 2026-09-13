package io.ledgerflow.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.contracts.TransactionReconciliationRequestedEvent;
import io.ledgerflow.contracts.TransactionStatus;
import io.ledgerflow.contracts.TransactionStatusChangedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class ReconciliationRequestedListener {
    private static final Logger log = LoggerFactory.getLogger(ReconciliationRequestedListener.class);
    private static final String STATUS_TOPIC = "ledgerflow.transaction.status.v1";

    private final ObjectMapper objectMapper;
    private final ProviderClient provider;
    private final KafkaTemplate<String, String> kafka;
    private final Counter resolved;
    private final Counter unresolved;
    private final Counter failures;

    public ReconciliationRequestedListener(
            ObjectMapper objectMapper,
            ProviderClient provider,
            KafkaTemplate<String, String> kafka,
            MeterRegistry registry) {
        this.objectMapper = objectMapper;
        this.provider = provider;
        this.kafka = kafka;
        this.resolved = Counter.builder("ledgerflow.reconciliation.resolved")
                .description("Reconciliation lookups that emitted a terminal status")
                .register(registry);
        this.unresolved = Counter.builder("ledgerflow.reconciliation.unresolved")
                .description("Reconciliation lookups with no recorded provider decision")
                .register(registry);
        this.failures = Counter.builder("ledgerflow.reconciliation.failure")
                .description("Reconciliation requests that exhausted processing retries")
                .register(registry);
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 500, multiplier = 2.0, maxDelay = 5000),
            dltTopicSuffix = "-dlt")
    @KafkaListener(
            topics = "ledgerflow.transaction.reconciliation.requested.v1",
            groupId = "transaction-reconciliation-processor-v1")
    public void onRequested(String payload) throws Exception {
        var request = objectMapper.readValue(payload, TransactionReconciliationRequestedEvent.class);
        var payment = provider.findPayment(request.transactionId());
        if (payment.isEmpty() || payment.get().decision() == null) {
            unresolved.increment();
            return;
        }

        var decision = payment.get().decision();
        var status = decision.approved() ? TransactionStatus.COMPLETED : TransactionStatus.FAILED;
        var event = new TransactionStatusChangedEvent(
                UUID.randomUUID(),
                1,
                request.transactionId(),
                status,
                decision.providerReference(),
                decision.failureCode(),
                request.traceId(),
                Instant.now());
        kafka.send(STATUS_TOPIC, event.transactionId().toString(), objectMapper.writeValueAsString(event))
                .get(5, TimeUnit.SECONDS);
        resolved.increment();
    }

    @DltHandler
    public void onDeadLetter(String payload) throws Exception {
        var request = objectMapper.readValue(payload, TransactionReconciliationRequestedEvent.class);
        failures.increment();
        log.error("Reconciliation request exhausted retries transactionId={} eventId={}",
                request.transactionId(), request.eventId());
    }
}
