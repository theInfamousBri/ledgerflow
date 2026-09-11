package io.ledgerflow.contracts;

import java.math.BigDecimal;
import java.util.UUID;

public record ProviderPaymentRequest(
        UUID transactionId,
        BigDecimal amount,
        String currency) {
}

