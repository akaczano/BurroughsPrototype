package com.viasat.burroughs.service.model.list;

public class Table {
    private String name;
    private String topic;
    private Format format;
    private boolean isWindowed;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Format getFormat() {
        return format;
    }

    public void setFormat(Format format) {
        this.format = format;
    }

    public boolean isWindowed() {
        return isWindowed;
    }

    public void setWindowed(boolean windowed) {
        isWindowed = windowed;
    }

    @Override
    public String toString() {
        return name;
    }
}
