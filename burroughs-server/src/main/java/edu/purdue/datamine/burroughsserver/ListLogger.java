package edu.purdue.datamine.burroughsserver;

import com.viasat.burroughs.ILogger;
import edu.purdue.datamine.burroughsserver.model.Message;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Logs console messages to a thread-sage list with
 * timestamps.
 */
public class ListLogger implements ILogger {

    private final CopyOnWriteArrayList<Message> messages;
    private String inProgress = "";
    private Message.Color inProgressColor;

    public ListLogger() {
        messages = new CopyOnWriteArrayList<>();
    }

    /**
     * Return only the messages that occurred after the last query
     * @param lastQuery The time (in ms) of the last query
     * @return The messages as an object array
     */
    public Object[] getMessages(long lastQuery) {
        return messages.stream().filter(m -> m.getTime() > lastQuery).toArray();
    }

    private void flush() {
        messages.add(new Message(inProgress, System.currentTimeMillis() - 1, inProgressColor));
        inProgress = "";
    }

    public void writeLine(String line) {
        flush();
        messages.add(new Message(line, System.currentTimeMillis(),  Message.Color.NONE));
    }
    public void writeLineRed(String line) {
        flush();
        messages.add(new Message(line, System.currentTimeMillis(),  Message.Color.RED));
    }
    public void writeLineGreen(String line) {
        flush();
        messages.add(new Message(line, System.currentTimeMillis(),  Message.Color.GREEN));
    }
    public void writeLineYellow(String line) {
        flush();
        messages.add(new Message(line, System.currentTimeMillis(),  Message.Color.YELLOW));
    }

    private void write (String text, Message.Color color) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '\n') {
                messages.add(new Message(inProgress, System.currentTimeMillis(), color));
                inProgress = "";
            }
            inProgress += text.charAt(i);
            inProgressColor = color;
        }
    }

    public void write(String text) {
        write(text, Message.Color.NONE);
    }
    public void writeRed(String text) {
        write(text, Message.Color.RED);
    }
    public void writeGreen(String text) {
        write(text, Message.Color.GREEN);
    }
    public void writeYellow(String text) {
        write(text, Message.Color.YELLOW);
    }
}
