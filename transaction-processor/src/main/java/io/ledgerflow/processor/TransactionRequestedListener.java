package io.ledgerflow.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.contracts.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class TransactionRequestedListener {
    private static final Logger log = LoggerFactory.getLogger(TransactionRequestedListener.class);
    private static final String STATUS_TOPIC = "ledgerflow.transaction.status.v1";

    private final ObjectMapper objectMapper;
    private final ProviderClient provider;
    private final KafkaTemplate<String, String> kafka;

    public TransactionRequestedListener(ObjectMapper objectMapper, ProviderClient provider,
                                        KafkaTemplate<String, String> kafka) {
        this.objectMapper = objectMapper;
        this.provider = provider;
        this.kafka = kafka;
    }

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 500, multiplier = 2.0, maxDelay = 5000),
            dltTopicSuffix = "-dlt")
    @KafkaListener(topics = "ledgerflow.transaction.requested.v1", groupId = "transaction-processor-v1")
    public void onRequested(String payload) throws Exception {
        var request = objectMapper.readValue(payload, TransactionRequestedEvent.class);
        publish(status(request, TransactionStatus.PROCESSING, null, null));

        var response = provider.process(request);
        if (response == null) throw new IllegalStateException("Provider returned an empty response");

        var finalStatus = response.approved() ? TransactionStatus.COMPLETED : TransactionStatus.FAILED;
        publish(status(request, finalStatus, response.providerReference(), response.failureCode()));
    }

    @DltHandler
    public void onDeadLetter(String payload) throws Exception {
        var request = objectMapper.readValue(payload, TransactionRequestedEvent.class);
        log.error("Requested event exhausted retries transactionId={} eventId={}",
                request.transactionId(), request.eventId());
        publish(status(request, TransactionStatus.FAILED, null, "RETRIES_EXHAUSTED"));
    }

    private TransactionStatusChangedEvent status(TransactionRequestedEvent request, TransactionStatus status,
                                                 String providerReference, String failureCode) {
        return new TransactionStatusChangedEvent(UUID.randomUUID(), 1, request.transactionId(), status,
                providerReference, failureCode, request.traceId(), Instant.now());
    }

    private void publish(TransactionStatusChangedEvent event) throws Exception {
        kafka.send(STATUS_TOPIC, event.transactionId().toString(), objectMapper.writeValueAsString(event))
                .get(5, TimeUnit.SECONDS);
    }
}
