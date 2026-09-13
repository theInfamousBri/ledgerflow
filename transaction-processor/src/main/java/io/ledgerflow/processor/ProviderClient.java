package io.ledgerflow.processor;

import io.ledgerflow.contracts.*;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.util.Optional;
import java.util.UUID;

@Component
public class ProviderClient {
    private final RestClient restClient;

    public ProviderClient(RestClient restClient) {
        this.restClient = restClient;
    }

    @CircuitBreaker(name = "paymentProvider")
    public ProviderPaymentResponse process(TransactionRequestedEvent event) {
        return restClient.post()
                .uri("/provider/payments")
                .header("Idempotency-Key", event.transactionId().toString())
                .body(new ProviderPaymentRequest(event.transactionId(), event.amount(), event.currency()))
                .retrieve()
                .body(ProviderPaymentResponse.class);
    }

    @CircuitBreaker(name = "paymentProvider")
    public Optional<ProviderPaymentStatus> findPayment(UUID transactionId) {
        try {
            return Optional.ofNullable(restClient.get()
                    .uri("/provider/payments/{transactionId}", transactionId)
                    .retrieve()
                    .body(ProviderPaymentStatus.class));
        } catch (HttpClientErrorException.NotFound exception) {
            return Optional.empty();
        }
    }
}

