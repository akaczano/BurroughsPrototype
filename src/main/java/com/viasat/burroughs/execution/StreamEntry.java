package com.viasat.burroughs.execution;

public class StreamEntry {
    private final String streamName;
    private final boolean deleteTopic;

    public StreamEntry(String streamName, boolean deleteTopic) {
        this.streamName = streamName;
        this.deleteTopic = deleteTopic;
    }

    public String getStreamName() {
        return streamName;
    }

    public boolean isDeleteTopic() {
        return deleteTopic;
    }
}
