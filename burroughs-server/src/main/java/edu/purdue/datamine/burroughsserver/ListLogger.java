package edu.purdue.datamine.burroughsserver;

import com.viasat.burroughs.logging.LogEntry;
import com.viasat.burroughs.logging.Logger;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Logs console messages to a thread-sage list with
 * timestamps.
 */
public class ListLogger extends Logger {

    private final CopyOnWriteArrayList<LogEntry > messages;
    private String inProgress = "";
    private int inProgressLevel;
    private int inProgressColor;

    public ListLogger() {
        messages = new CopyOnWriteArrayList<>();
    }

    /**
     * Return only the messages that occurred after the last query
     * @param lastQuery The time (in ms) of the last query
     * @return The messages as an object array
     */
    public Object[] getMessages(long lastQuery) {
        return messages
                .stream()
                .filter(m -> m.getTime() > lastQuery)
                .toArray();
    }

    @Override
    public void write(String text, int color, int debugLevel) {
        for (int i = 0; i < text.length(); i++) {
            if (text.charAt(i) == '\n') {
                messages.add(new LogEntry(inProgress, System.currentTimeMillis(), inProgressColor, inProgressLevel));
                inProgress = "";
            }
            inProgress += text.charAt(i);
            inProgressColor = color;
            inProgressLevel = debugLevel;
        }
    }

    @Override
    public void writeLine(String line, int color, int debugLevel) {
        flush();
        messages.add(new LogEntry(line, System.currentTimeMillis(), color, debugLevel));
    }

    @Override
    public void clearLog() {
        this.messages.clear();
    }

    private void flush() {
        messages.add(new LogEntry(inProgress, System.currentTimeMillis() - 1, inProgressColor, inProgressLevel));
        inProgress = "";
    }
}
