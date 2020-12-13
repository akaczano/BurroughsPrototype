package com.viasat.burroughs.service.model.description;

public class Query {
    private String queryString;
    private String[] sinks;
    private String[] sinkKafkaTopics;
    private String id;
    private String state;

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public String[] getSink() {
        return sinks;
    }

    public void setSink(String[] sink) {
        this.sinks = sink;
    }

    public String[] getSinkKafkaTopics() {
        return sinkKafkaTopics;
    }

    public void setSinkKafkaTopics(String[] sinkKafkaTopics) {
        this.sinkKafkaTopics = sinkKafkaTopics;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
