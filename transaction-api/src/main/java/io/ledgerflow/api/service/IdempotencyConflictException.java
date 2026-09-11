package io.ledgerflow.api.service;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.CONFLICT)
public class IdempotencyConflictException extends RuntimeException {
    public IdempotencyConflictException() {
        super("The idempotency key was already used with a different request");
    }
}

