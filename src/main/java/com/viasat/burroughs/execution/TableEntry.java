package com.viasat.burroughs.execution;

public class TableEntry {
    private String topicName;
    private final String tableName;
    private final boolean deleteTopic;

    public TableEntry(String tableName, boolean deleteTopic) {
        this.tableName = tableName;
        this.deleteTopic = deleteTopic;
        this.topicName = null;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isDeleteTopic() {
        return deleteTopic;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getTopicName() {
        return this.topicName;
    }

}
