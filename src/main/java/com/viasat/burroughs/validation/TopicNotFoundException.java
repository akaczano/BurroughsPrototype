package com.viasat.burroughs.validation;

/**
 * Thrown when a topic is referenced that doesn't exist
 */
public class TopicNotFoundException extends Exception {
    public TopicNotFoundException(String topic) {
        super("Topic not found: " + topic);
    }
}
