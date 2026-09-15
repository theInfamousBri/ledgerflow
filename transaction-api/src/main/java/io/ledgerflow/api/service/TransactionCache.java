package io.ledgerflow.api.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ledgerflow.api.web.TransactionResponse;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

@Component
public class TransactionCache {
    private static final Logger log = LoggerFactory.getLogger(TransactionCache.class);
    private static final String KEY_PREFIX = "ledgerflow:transaction:v1:";

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final Duration ttl;
    private final Counter hits;
    private final Counter misses;
    private final Counter writes;
    private final Counter evictions;
    private final Counter failures;

    public TransactionCache(StringRedisTemplate redis,
                            ObjectMapper objectMapper,
                            @Value("${ledgerflow.cache.transaction-ttl:30s}") Duration ttl,
                            MeterRegistry meterRegistry) {
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("Transaction cache TTL must be positive");
        }
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.ttl = ttl;
        this.hits = meterRegistry.counter("ledgerflow.cache.transaction.hit");
        this.misses = meterRegistry.counter("ledgerflow.cache.transaction.miss");
        this.writes = meterRegistry.counter("ledgerflow.cache.transaction.write");
        this.evictions = meterRegistry.counter("ledgerflow.cache.transaction.eviction");
        this.failures = meterRegistry.counter("ledgerflow.cache.transaction.failure");
    }

    public Optional<TransactionResponse> get(UUID transactionId) {
        String key = key(transactionId);
        try {
            String json = redis.opsForValue().get(key);
            if (json == null) {
                misses.increment();
                return Optional.empty();
            }
            try {
                TransactionResponse response = objectMapper.readValue(json, TransactionResponse.class);
                hits.increment();
                return Optional.of(response);
            } catch (JsonProcessingException exception) {
                failures.increment();
                log.warn("Discarding unreadable cached transaction {}", transactionId, exception);
                delete(key, transactionId);
                return Optional.empty();
            }
        } catch (DataAccessException exception) {
            failures.increment();
            log.warn("Redis read failed for transaction {}; falling back to PostgreSQL",
                    transactionId, exception);
            return Optional.empty();
        }
    }

    public void put(TransactionResponse response) {
        try {
            String json = objectMapper.writeValueAsString(response);
            redis.opsForValue().set(key(response.id()), json, ttl);
            writes.increment();
        } catch (JsonProcessingException | DataAccessException exception) {
            failures.increment();
            log.warn("Could not cache transaction {}; PostgreSQL remains authoritative",
                    response.id(), exception);
        }
    }

    public void evictAfterCommit(UUID transactionId) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            evict(transactionId);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                evict(transactionId);
            }
        });
    }

    public void evict(UUID transactionId) {
        delete(key(transactionId), transactionId);
    }

    public String key(UUID transactionId) {
        return KEY_PREFIX + transactionId;
    }

    private void delete(String key, UUID transactionId) {
        try {
            redis.delete(key);
            evictions.increment();
        } catch (DataAccessException exception) {
            failures.increment();
            log.warn("Could not evict cached transaction {}; TTL will bound staleness",
                    transactionId, exception);
        }
    }
}
