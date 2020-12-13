package com.viasat.burroughs.service.model.body;

public class StatementHolder {

    private String ksql;
    private StreamProperties streamsProperties;

    public StatementHolder(String ksql, StreamProperties properties) {
        this.ksql = ksql;
        this.streamsProperties = properties;
    }

    public String getKsql() {
        return ksql;
    }

    public void setKsql(String ksql) {
        this.ksql = ksql;
    }

    public StreamProperties getProperties() {
        return streamsProperties;
    }

    public void setProperties(StreamProperties properties) {
        this.streamsProperties = properties;
    }
}
