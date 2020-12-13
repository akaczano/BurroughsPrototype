package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.model.StatementError;

public class ExecutionException extends RuntimeException {
    public ExecutionException(String message) {
        super(message);
    }

    public ExecutionException(StatementError e) {
        super(e.getMessage());
    }
}
