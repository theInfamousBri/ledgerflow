package io.ledgerflow.provider;

import io.ledgerflow.contracts.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/provider/payments")
public class ProviderController {
    private final Map<UUID, ProviderPaymentResponse> decisions = new ConcurrentHashMap<>();
    private final Map<UUID, AtomicInteger> requestCounts = new ConcurrentHashMap<>();
    private final Map<UUID, List<Instant>> attemptTimes = new ConcurrentHashMap<>();
    private final Map<UUID, AtomicInteger> remainingPlannedFailures = new ConcurrentHashMap<>();
    private final Map<UUID, Long> processingDelays = new ConcurrentHashMap<>();
    private final Queue<Integer> nextFailurePlans = new ConcurrentLinkedQueue<>();
    private final Queue<Long> nextDelayPlans = new ConcurrentLinkedQueue<>();
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
        attemptTimes.computeIfAbsent(request.transactionId(), ignored -> new CopyOnWriteArrayList<>())
                .add(Instant.now());
        var existing = decisions.get(request.transactionId());
        if (existing != null) return existing;
        long processingDelay = processingDelays.computeIfAbsent(request.transactionId(),
                ignored -> nextProcessingDelay());
        Thread.sleep(processingDelay);
        existing = decisions.get(request.transactionId());
        if (existing != null) return existing;
        var plannedFailures = remainingPlannedFailures.computeIfAbsent(request.transactionId(),
                ignored -> new AtomicInteger(nextFailureCount()));
        if (plannedFailures.getAndUpdate(remaining -> Math.max(0, remaining - 1)) > 0) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
                    "Simulated planned transient failure");
        }
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Simulated transient failure");
        }
        var decision = new ProviderPaymentResponse(true, "sim-" + UUID.randomUUID(), null);
        decisions.putIfAbsent(request.transactionId(), decision);
        return decisions.get(request.transactionId());
    }

    @PostMapping("/simulation/fail-next")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void failNext(@RequestParam int attempts) {
        if (attempts < 1 || attempts > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "attempts must be between 1 and 100");
        }
        nextFailurePlans.add(attempts);
    }

    @PostMapping("/simulation/delay-next")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delayNext(@RequestParam long milliseconds) {
        if (milliseconds < 1 || milliseconds > 60_000) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "milliseconds must be between 1 and 60000");
        }
        nextDelayPlans.add(milliseconds);
    }

    @GetMapping("/{transactionId}")
    public ProviderPaymentStatus get(@PathVariable UUID transactionId) {
        var decision = decisions.get(transactionId);
        var requestCount = requestCounts.get(transactionId);
        if (decision == null && requestCount == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Provider payment not found");
        }
        return new ProviderPaymentStatus(transactionId, decision, requestCount == null ? 0 : requestCount.get(),
                List.copyOf(attemptTimes.getOrDefault(transactionId, List.of())));
    }

    private int nextFailureCount() {
        Integer failureCount = nextFailurePlans.poll();
        return failureCount == null ? 0 : failureCount;
    }

    private long nextProcessingDelay() {
        Long processingDelay = nextDelayPlans.poll();
        return processingDelay == null ? latencyMs : processingDelay;
    }

    public record ProviderPaymentStatus(
            UUID transactionId,
            ProviderPaymentResponse decision,
            int requestCount,
            List<Instant> attemptedAt) {}
}
