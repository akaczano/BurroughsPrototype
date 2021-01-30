package com.viasat.burroughs.service.model.burroughs;

public class BurroughsConnection {

    private String ksqlHost;
    private String dBHost;
    private boolean ksqlConnected;
    private boolean dbConnected;

    public String getKsqlHost() {
        return ksqlHost;
    }

    public void setKsqlHost(String ksqlHost) {
        this.ksqlHost = ksqlHost;
    }

    public String getdBHost() {
        return dBHost;
    }

    public void setdBHost(String dBHost) {
        this.dBHost = dBHost;
    }

    public boolean isKsqlConnected() {
        return ksqlConnected;
    }

    public void setKsqlConnected(boolean ksqlConnected) {
        this.ksqlConnected = ksqlConnected;
    }

    public boolean isDbConnected() {
        return dbConnected;
    }

    public void setDbConnected(boolean dbConnected) {
        this.dbConnected = dbConnected;
    }
}
