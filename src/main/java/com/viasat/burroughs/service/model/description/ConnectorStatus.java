package com.viasat.burroughs.service.model.description;

public class ConnectorStatus {
    private String name;
    private Connector connector;
    private ConnectorTask[] tasks;
    private String type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Connector getConnector() {
        return connector;
    }

    public void setConnector(Connector connector) {
        this.connector = connector;
    }

    public ConnectorTask[] getTasks() {
        return tasks;
    }

    public void setTasks(ConnectorTask[] tasks) {
        this.tasks = tasks;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
