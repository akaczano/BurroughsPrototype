package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.model.StatementError;

/**
 * Any error that occurs during query processing
 */
public class ExecutionException extends RuntimeException {
    public ExecutionException(String message) {
        super(message);
	
	//lines added below
	System.out.println("An error has occured during query processing.  Please type '.debug <level number>' in the CLI to view traceback. '\n\t'<level number>: 1 = displays general traceback of SQL query transformation. '\n\t' 2 = displays more detailed traceback of SQL query transformation");
    }

    /**
     * Creates an execution exception from a StatementError object
     * @param e A statement error object received from ksqlDB
     */
    public ExecutionException(StatementError e) {
        super(e.getMessage());
	//lines added below
    	System.out.println("An error has occured during query processing.  Please type '.debug <level number>' in the CLI to view traceback. '\n\t'<level number>: 1 = displays general traceback of SQL query transformation. '\n\t' 2 = displays more detailed traceback of SQL query transformation");

    }

}
