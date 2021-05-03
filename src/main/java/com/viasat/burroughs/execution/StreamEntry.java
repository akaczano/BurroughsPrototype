package com.viasat.burroughs.execution;

public class StreamEntry {
    private String topicName;
    private final String streamName;
    private final boolean deleteTopic;

    public StreamEntry(String streamName, String topicName, boolean deleteTopic) {
        this.streamName = streamName;
        this.deleteTopic = deleteTopic;
        this.topicName = topicName;
    }

    public String getStreamName() {
        return streamName;
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
