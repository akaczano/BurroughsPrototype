package com.viasat.burroughs.producer;

/**
 * Runtime exception thrown during producer operation.
 */
public class ProducerException extends RuntimeException {
    public ProducerException(String message) {
        super(message);
    }
}
