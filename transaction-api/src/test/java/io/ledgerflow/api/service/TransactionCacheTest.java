package io.ledgerflow.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.api.web.TransactionResponse;
import io.ledgerflow.contracts.TransactionStatus;
import io.ledgerflow.contracts.TransactionType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TransactionCacheTest {
    private StringRedisTemplate redis;
    private ValueOperations<String, String> values;
    private SimpleMeterRegistry metrics;
    private TransactionCache cache;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(StringRedisTemplate.class);
        values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        metrics = new SimpleMeterRegistry();
        cache = new TransactionCache(redis, new ObjectMapper().findAndRegisterModules(),
                Duration.ofSeconds(30), metrics);
    }

    @Test
    void returnsCachedTransactionAndRecordsHit() throws Exception {
        TransactionResponse response = response();
        when(values.get(cache.key(response.id())))
                .thenReturn(new ObjectMapper().findAndRegisterModules().writeValueAsString(response));

        assertThat(cache.get(response.id())).contains(response);
        assertThat(metrics.get("ledgerflow.cache.transaction.hit").counter().count()).isEqualTo(1);
        assertThat(metrics.get("ledgerflow.cache.transaction.miss").counter().count()).isZero();
    }

    @Test
    void redisFailureBecomesCacheMissInsteadOfEscaping() {
        UUID transactionId = UUID.randomUUID();
        when(values.get(anyString())).thenThrow(new RedisConnectionFailureException("redis unavailable"));

        assertThat(cache.get(transactionId)).isEmpty();
        assertThat(metrics.get("ledgerflow.cache.transaction.failure").counter().count()).isEqualTo(1);
    }

    @Test
    void defersEvictionUntilDatabaseTransactionCommits() {
        UUID transactionId = UUID.randomUUID();
        TransactionSynchronizationManager.initSynchronization();
        try {
            cache.evictAfterCommit(transactionId);

            verify(redis, never()).delete(anyString());
            TransactionSynchronizationManager.getSynchronizations()
                    .forEach(TransactionSynchronization::afterCommit);
            verify(redis).delete(cache.key(transactionId));
        } finally {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    private TransactionResponse response() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.parse("2026-09-14T12:00:00Z");
        return new TransactionResponse(id, "acct-cache", new BigDecimal("19.95"), "USD",
                TransactionType.PAYMENT, TransactionStatus.COMPLETED, "sim-cache", null,
                now, now, List.of(
                new TransactionResponse.HistoryItem(TransactionStatus.PENDING, "Request accepted", now),
                new TransactionResponse.HistoryItem(TransactionStatus.COMPLETED, null, now)));
    }
}
