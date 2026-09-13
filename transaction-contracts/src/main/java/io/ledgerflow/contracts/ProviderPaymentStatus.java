package io.ledgerflow.contracts;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ProviderPaymentStatus(
        UUID transactionId,
        ProviderPaymentResponse decision,
        int requestCount,
        List<Instant> attemptedAt) {
}
