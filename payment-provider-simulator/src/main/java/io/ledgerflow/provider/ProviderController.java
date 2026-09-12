package io.ledgerflow.provider;

import io.ledgerflow.contracts.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/provider/payments")
public class ProviderController {
    private final Map<UUID, ProviderPaymentResponse> decisions = new ConcurrentHashMap<>();
    private final Map<UUID, AtomicInteger> requestCounts = new ConcurrentHashMap<>();
    private final double failureRate;
    private final long latencyMs;

    public ProviderController(@Value("${provider.failure-rate:0.0}") double failureRate,
                              @Value("${provider.latency-ms:100}") long latencyMs) {
        if (failureRate < 0 || failureRate > 1) throw new IllegalArgumentException("failure-rate must be 0..1");
        if (latencyMs < 0) throw new IllegalArgumentException("latency-ms must be non-negative");
        this.failureRate = failureRate;
        this.latencyMs = latencyMs;
    }

    @PostMapping
    public ProviderPaymentResponse process(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestBody ProviderPaymentRequest request) throws InterruptedException {
        if (!request.transactionId().toString().equals(idempotencyKey)) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Idempotency key must match transaction ID");
        }
        requestCounts.computeIfAbsent(request.transactionId(), ignored -> new AtomicInteger()).incrementAndGet();
        Thread.sleep(latencyMs);
        var existing = decisions.get(request.transactionId());
        if (existing != null) return existing;
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Simulated transient failure");
        }
        var decision = new ProviderPaymentResponse(true, "sim-" + UUID.randomUUID(), null);
        decisions.putIfAbsent(request.transactionId(), decision);
        return decisions.get(request.transactionId());
    }

    @GetMapping("/{transactionId}")
    public ProviderPaymentStatus get(@PathVariable UUID transactionId) {
        var decision = decisions.get(transactionId);
        if (decision == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Provider payment not found");
        }
        int requestCount = requestCounts.getOrDefault(transactionId, new AtomicInteger()).get();
        return new ProviderPaymentStatus(transactionId, decision, requestCount);
    }

    public record ProviderPaymentStatus(
            UUID transactionId,
            ProviderPaymentResponse decision,
            int requestCount) {}
}
