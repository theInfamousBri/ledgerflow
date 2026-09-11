package io.ledgerflow.api.web;

import io.ledgerflow.api.service.TransactionService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.*;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import java.net.URI;
import java.util.UUID;

@Validated
@RestController
@RequestMapping("/transactions")
public class TransactionController {
    private final TransactionService service;

    public TransactionController(TransactionService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<TransactionResponse> create(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 100)
            @Pattern(regexp = "[\\x21-\\x7E]+") String idempotencyKey,
            @RequestHeader(value = "X-Trace-Id", required = false) String suppliedTraceId,
            @Valid @RequestBody CreateTransactionRequest request) {
        String traceId = suppliedTraceId == null || suppliedTraceId.isBlank()
                ? UUID.randomUUID().toString() : suppliedTraceId;
        var result = service.create(idempotencyKey, request, traceId);
        var status = result.created() ? HttpStatus.ACCEPTED : HttpStatus.OK;
        return ResponseEntity.status(status)
                .location(URI.create("/transactions/" + result.transaction().id()))
                .header("X-Trace-Id", traceId)
                .body(result.transaction());
    }

    @GetMapping("/{id}")
    public TransactionResponse get(@PathVariable UUID id) {
        return service.get(id);
    }
}
