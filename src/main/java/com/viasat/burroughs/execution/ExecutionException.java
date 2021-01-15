package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.model.StatementError;

/**
 * Any error that occurs during query processing
 */
public class ExecutionException extends RuntimeException {
    public ExecutionException(String message) {
        super(message);
    }

    /**
     * Creates an execution exception from a StatementError object
     * @param e A statement error object received from ksqlDB
     */
    public ExecutionException(StatementError e) {
        super(e.getMessage());
    }
}
