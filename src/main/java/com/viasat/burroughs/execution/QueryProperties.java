package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;

public class QueryProperties {

    private String id;
    private String streamName;
    private String topicName;
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

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }
}
