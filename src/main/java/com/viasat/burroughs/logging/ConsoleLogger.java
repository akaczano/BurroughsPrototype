package com.viasat.burroughs.logging;


import java.util.ArrayList;

public class ConsoleLogger extends Logger {

    // Character sequences used to change text color
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_RESET = "\u001B[0m";

    private ArrayList<LogEntry> debugLevels = new ArrayList<>();


    @Override
    public void write(String text, int color, int debugLevel) {
        if (debugLevel == NORMAL) {
            String modifier = "";
            if (color == ERROR) {
                modifier = ANSI_RED;
            }
            else if (color == SUCCESS) {
                modifier = ANSI_GREEN;
            }
            else if (color == WARNING) {
                modifier = ANSI_YELLOW;
            }
            System.out.printf("%s%s%s", modifier, text, ANSI_RESET);
        }
        else {
            debugLevels.add(new LogEntry(text, System.currentTimeMillis(), color, debugLevel));
        }
    }

    @Override
    public void writeLine(String text, int color, int debugLevel) {
        if (debugLevel == NORMAL) {
            String modifier = "";
            if (color == ERROR) {
                modifier = ANSI_RED;
            }
            else if (color == SUCCESS) {
                modifier = ANSI_GREEN;
            }
            else if (color == WARNING) {
                modifier = ANSI_YELLOW;
            }
            System.out.printf("%s%s%s\n", modifier, text, ANSI_RESET);
        }
        else {
            debugLevels.add(new LogEntry(text + "\n", System.currentTimeMillis(), color, debugLevel));
        }
    }

    public void displayDebug(int level) {
        this.debugLevels
                .stream()
                .filter(e -> e.getDebugLevel() == level)
                .forEach(e -> {
                    System.out.print(e.getText());
                });
    }

    @Override
    public void clearLog() {
        debugLevels.clear();
    }
}
