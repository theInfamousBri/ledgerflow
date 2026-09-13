# ADR-006: Provider timeout semantics

Status: accepted

## Decision

Apply bounded, externally configurable connect and read timeouts to provider HTTP calls. A timed-out request is retried with the same LedgerFlow transaction ID as its idempotency key, allowing a later attempt to retrieve a decision completed after the original response window closed.

Production defaults are a one-second connect timeout and a two-second read timeout.

## Rationale

An HTTP timeout proves only that LedgerFlow did not receive a response in time. It does not prove that the provider abandoned the operation. Retrying with a new identifier could therefore create a second payment, while retrying with the original idempotency key allows the provider to return its stored outcome safely.

## Consequences

- Provider calls cannot occupy processor resources indefinitely.
- A retry can recover the original provider reference without creating another logical payment effect.
- Timeouts participate in the same bounded exponential-backoff policy as transient HTTP failures.
- Timeout values can be tuned by environment without rebuilding the processor.
- If every attempt times out, the provider outcome may still be ambiguous. The MVP routes exhausted work through its DLT policy; reconciliation and an explicit unknown-outcome model remain future hardening options.
- The E2E profile uses a shorter read timeout so the real timeout behavior is tested without adding several seconds per attempt.
