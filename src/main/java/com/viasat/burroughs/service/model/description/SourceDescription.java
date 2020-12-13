package com.viasat.burroughs.service.model.description;

import com.viasat.burroughs.service.model.list.Format;

public class SourceDescription {
    private String name;
    private Query[] readQueries;
    private Query[] writeQueries;
    private Field[] fields;
    private SourceType type;
    private String key;
    private String timestamp;
    private Format format;
    private String topic;
    private boolean extended;
    private String statistics;
    private String errorStats;
    private int replication;
    private int partitions;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Query[] getReadQueries() {
        return readQueries;
    }

    public void setReadQueries(Query[] readQueries) {
        this.readQueries = readQueries;
    }

    public Query[] getWriteQueries() {
        return writeQueries;
    }

    public void setWriteQueries(Query[] writeQueries) {
        this.writeQueries = writeQueries;
    }

    public Field[] getFields() {
        return fields;
    }

    public void setFields(Field[] fields) {
        this.fields = fields;
    }

    public SourceType getType() {
        return type;
    }

    public void setType(SourceType type) {
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public boolean isExtended() {
        return extended;
    }

    public void setExtended(boolean extended) {
        this.extended = extended;
    }

    public String getStatistics() {
        return statistics;
    }

    public void setStatistics(String statistics) {
        this.statistics = statistics;
    }

    public String getErrorStats() {
        return errorStats;
    }

    public void setErrorStats(String errorStats) {
        this.errorStats = errorStats;
    }

    public int getReplication() {
        return replication;
    }

    public void setReplication(int replication) {
        this.replication = replication;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }
}
