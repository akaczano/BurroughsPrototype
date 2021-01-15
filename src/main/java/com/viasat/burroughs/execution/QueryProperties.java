package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;

/**
 * This is a bean meant to contain all of the information a query needs to execute.
 * It used to contain a lot more stuff.
 */
public class QueryProperties {

    private String id;
    private DBProvider dbInfo;

    public DBProvider getDbInfo() {
        return dbInfo;
    }

    public void setDbInfo(DBProvider dbInfo) {
        this.dbInfo = dbInfo;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
