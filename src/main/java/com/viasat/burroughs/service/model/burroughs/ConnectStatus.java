package com.viasat.burroughs.service.model.burroughs;

import java.util.List;

public class ConnectStatus {
    private boolean connectorRunning;
    List<String> errors;

    public boolean isConnectorRunning() {
        return connectorRunning;
    }

    public void setConnectorRunning(boolean connectorRunning) {
        this.connectorRunning = connectorRunning;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
}
