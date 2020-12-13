package com.viasat.burroughs.service.model.list;

public class Query {
    private String queryString;
    private String sinks;
    private String id;

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    public String getSinks() {
        return sinks;
    }

    public void setSinks(String sinks) {
        this.sinks = sinks;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
