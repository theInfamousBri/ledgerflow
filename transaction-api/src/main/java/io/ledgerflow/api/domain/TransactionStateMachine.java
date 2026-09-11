package io.ledgerflow.api.domain;

import io.ledgerflow.contracts.TransactionStatus;
import java.util.Set;

public final class TransactionStateMachine {
    private TransactionStateMachine() {}

    public static boolean isAllowed(TransactionStatus current, TransactionStatus next) {
        if (current == next) return true;
        return switch (current) {
            case PENDING -> next == TransactionStatus.PROCESSING;
            case PROCESSING -> Set.of(TransactionStatus.COMPLETED, TransactionStatus.FAILED).contains(next);
            case COMPLETED, FAILED -> false;
        };
    }

    public static boolean isStale(TransactionStatus current, TransactionStatus incoming) {
        return rank(incoming) < rank(current);
    }

    private static int rank(TransactionStatus status) {
        return switch (status) {
            case PENDING -> 0;
            case PROCESSING -> 1;
            case COMPLETED, FAILED -> 2;
        };
    }
}
