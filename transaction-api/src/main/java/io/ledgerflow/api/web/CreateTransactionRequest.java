package io.ledgerflow.api.web;

import io.ledgerflow.contracts.TransactionType;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;

public record CreateTransactionRequest(
        @NotBlank @Size(max = 100) String accountId,
        @NotNull @DecimalMin(value = "0.01") @Digits(integer = 17, fraction = 2) BigDecimal amount,
        @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency,
        @NotNull TransactionType type) {
}

