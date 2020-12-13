package com.viasat.burroughs.service.model.list;

import com.viasat.burroughs.service.model.StatementResponse;

public class ListResponse extends StatementResponse {

    private Stream[] streams;
    private Table[] tables;
    private Topic[] topics;
    private Connector[] connectors;
    private Query[] queries;

    public Topic[] getTopics() {
        return topics;
    }

    public void setTopics(Topic[] topics) {
        this.topics = topics;
    }

    public Stream[] getStreams() {
        return streams;
    }

    public void setStreams(Stream[] streams) {
        this.streams = streams;
    }

    public Table[] getTables() {
        return tables;
    }

    public void setTables(Table[] tables) {
        this.tables = tables;
    }

    public Connector[] getConnectors() {
        return connectors;
    }

    public void setConnectors(Connector[] connectors) {
        this.connectors = connectors;
    }
}
