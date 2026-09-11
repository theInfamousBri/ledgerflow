package io.ledgerflow.api.domain;

import io.ledgerflow.contracts.TransactionStatus;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class TransactionStateMachineTest {
    @Test
    void allowsExpectedHappyPath() {
        assertThat(TransactionStateMachine.isAllowed(TransactionStatus.PENDING, TransactionStatus.PROCESSING)).isTrue();
        assertThat(TransactionStateMachine.isAllowed(TransactionStatus.PROCESSING, TransactionStatus.COMPLETED)).isTrue();
    }

    @Test
    void preventsTerminalStateMutation() {
        assertThat(TransactionStateMachine.isAllowed(TransactionStatus.COMPLETED, TransactionStatus.FAILED)).isFalse();
        assertThat(TransactionStateMachine.isAllowed(TransactionStatus.FAILED, TransactionStatus.PROCESSING)).isFalse();
    }

    @Test
    void treatsDuplicateStateAsIdempotent() {
        assertThat(TransactionStateMachine.isAllowed(TransactionStatus.PROCESSING, TransactionStatus.PROCESSING)).isTrue();
    }

    @Test
    void recognizesLateNonTerminalEventsAsStale() {
        assertThat(TransactionStateMachine.isStale(TransactionStatus.COMPLETED, TransactionStatus.PROCESSING)).isTrue();
        assertThat(TransactionStateMachine.isStale(TransactionStatus.PROCESSING, TransactionStatus.PENDING)).isTrue();
        assertThat(TransactionStateMachine.isStale(TransactionStatus.COMPLETED, TransactionStatus.FAILED)).isFalse();
    }
}
