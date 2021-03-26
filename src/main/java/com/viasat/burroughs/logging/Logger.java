package com.viasat.burroughs.logging;

public abstract class Logger {

    private static Logger logger;

    public static Logger getLogger() {
        return logger;
    }

    public static void setLogger(Logger logger) {
        Logger.logger = logger;
    }

    public static final int DEFAULT = 0;
    public static final int ERROR = 1;
    public static final int SUCCESS = 2;
    public static final int WARNING = 3;

    // Debug Levels
    public static final int NORMAL = 4;
    public static final int LEVEL_1 = 5;
    public static final int LEVEL_2 = 6;

    public abstract void write(String text, int color, int debugLevel);
    public abstract void writeLine(String text, int color, int debugLevel);
    public abstract void clearLog();

    public void writeLine(String text) {
        writeLine(text, DEFAULT, NORMAL);
    }

    public void write(String text) {
        write(text, DEFAULT, NORMAL);
    }



}

