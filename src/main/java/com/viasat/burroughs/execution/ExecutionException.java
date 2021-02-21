package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.model.StatementError;

/**
 * Any error that occurs during query processing
 */
public class ExecutionException extends RuntimeException {
    public ExecutionException(String message) {
        super(message);
	
	//lines added below
	System.out.println("An error has occured during query processing.  Please type '.debug' in the CLI to view traceback.");
    }

    /**
     * Creates an execution exception from a StatementError object
     * @param e A statement error object received from ksqlDB
     */
    public ExecutionException(StatementError e) {
        super(e.getMessage());
	//lines added below
	System.out.println("An error has occured during query processing.  Please type '.debug' in the CLI to view traceback.");
    }
}
