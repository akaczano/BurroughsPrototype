package com.viasat.burroughs.service.model.description;

import com.google.gson.annotations.SerializedName;

public class ConnectorTask {
    private int id;
    private String state;

    @SerializedName("worker_id")
    private String workerId;

    private String trace;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public String getTrace() {
        return trace;
    }

    public void setTrace(String trace) {
        this.trace = trace;
    }
}
