package edu.purdue.datamine.burroughsserver.models;

public class Message {

    public enum Color {
        RED, GREEN, YELLOW, NONE
    }

    private final String text;
    private final long time;
    private final Color color;

    public Message(String text, long time, Color color) {
        this.text = text;
        this.time = time;
        this.color = color;
    }

    public String getText() {
        return text;
    }

    public long getTime() {
        return time;
    }

    public Color getColor() {
        return color;
    }
}
