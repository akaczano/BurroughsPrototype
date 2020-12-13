package com.viasat.burroughs.service.model.body;

import com.google.gson.annotations.SerializedName;

public class StreamProperties {

    @SerializedName("auto.offset.reset")
    private String autoOffsetReset;

    public StreamProperties() {

    }
    public StreamProperties(boolean fromBeginning) {

        this.autoOffsetReset = fromBeginning ? "earliest" : "latest";
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }
}
