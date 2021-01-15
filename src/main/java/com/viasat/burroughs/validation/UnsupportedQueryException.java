package com.viasat.burroughs.validation;

/**
 * Thrown when an unsupported query is run.
 */
public class UnsupportedQueryException extends Exception {
    public UnsupportedQueryException(String message) {
        super(message);
    }
}
