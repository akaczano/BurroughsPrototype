package com.viasat.burroughs.validation;

public class TopicNotFoundException extends Exception {
    public TopicNotFoundException(String topic) {
        super("Topic not found: " + topic);
    }
}
