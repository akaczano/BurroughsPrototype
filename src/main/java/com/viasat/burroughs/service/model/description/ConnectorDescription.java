package com.viasat.burroughs.service.model.description;

import com.viasat.burroughs.service.model.StatementResponse;

public class ConnectorDescription extends StatementResponse {
    private String connectorClass;
    private ConnectorStatus status;

    public String getConnectorClass() {
        return connectorClass;
    }

    public void setConnectorClass(String connectorClass) {
        this.connectorClass = connectorClass;
    }

    public ConnectorStatus getStatus() {
        return status;
    }

    public void setStatus(ConnectorStatus status) {
        this.status = status;
    }
}
