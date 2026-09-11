package io.ledgerflow.contracts;

public record ProviderPaymentResponse(
        boolean approved,
        String providerReference,
        String failureCode) {
}
