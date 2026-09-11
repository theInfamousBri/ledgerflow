package io.ledgerflow.processor;

import io.ledgerflow.contracts.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

@Component
public class ProviderClient {
    private final RestClient restClient;

    public ProviderClient(RestClient restClient) {
        this.restClient = restClient;
    }

    public ProviderPaymentResponse process(TransactionRequestedEvent event) {
        return restClient.post()
                .uri("/provider/payments")
                .header("Idempotency-Key", event.transactionId().toString())
                .body(new ProviderPaymentRequest(event.transactionId(), event.amount(), event.currency()))
                .retrieve()
                .body(ProviderPaymentResponse.class);
    }
}

