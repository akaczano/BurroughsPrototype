package com.viasat.burroughs.service.model.description;

import com.google.gson.annotations.SerializedName;

public class Connector {
    private String state;

    @SerializedName("worker_id")
    private String workerId;

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
}
