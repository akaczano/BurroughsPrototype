package com.viasat.burroughs.service.model.burroughs;

public class QueryStatus {
    private TableStatus tableStatus;
    private ConnectStatus connectorStatus;
    private String queryId;

    public TableStatus getTableStatus() {
        return tableStatus;
    }

    public void setTableStatus(TableStatus tableStatus) {
        this.tableStatus = tableStatus;
    }

    public ConnectStatus getConnectorStatus() {
        return connectorStatus;
    }

    public void setConnectorStatus(ConnectStatus connectorStatus) {
        this.connectorStatus = connectorStatus;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
}
