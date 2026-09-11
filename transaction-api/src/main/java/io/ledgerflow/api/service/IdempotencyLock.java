package io.ledgerflow.api.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class IdempotencyLock {
    private final JdbcTemplate jdbcTemplate;

    public IdempotencyLock(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void acquire(String key) {
        jdbcTemplate.query("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", resultSet -> null, key);
    }
}
