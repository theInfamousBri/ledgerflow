package io.ledgerflow.api.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.api.service.TransactionService;
import io.ledgerflow.contracts.TransactionStatusChangedEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionStatusListener {
    private final ObjectMapper objectMapper;
    private final TransactionService service;

    public TransactionStatusListener(ObjectMapper objectMapper, TransactionService service) {
        this.objectMapper = objectMapper;
        this.service = service;
    }

    @KafkaListener(topics = "ledgerflow.transaction.status.v1", groupId = "transaction-api-v1")
    public void onStatus(String payload) throws Exception {
        service.apply(objectMapper.readValue(payload, TransactionStatusChangedEvent.class));
    }
}

