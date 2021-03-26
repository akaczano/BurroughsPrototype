package com.viasat.burroughs.logging;

public class LogEntry {

    private final String text;
    private final long time;
    private final int color;
    private final int debugLevel;

    public LogEntry(String text, long time, int color, int debugLevel) {
        this.text = text;
        this.time = time;
        this.color = color;
        this.debugLevel = debugLevel;
    }

    public String getText() {
        return text;
    }

    public long getTime() {
        return time;
    }

    public int getColor() {
        return color;
    }

    public int getDebugLevel() {
        return debugLevel;
    }
}
