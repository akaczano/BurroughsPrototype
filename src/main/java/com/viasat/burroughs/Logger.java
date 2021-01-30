package com.viasat.burroughs;

public class Logger {

    private static ILogger logger;

    public static ILogger getLogger() {
        return logger;
    }

    public static void setLogger(ILogger logger) {
        Logger.logger = logger;
    }
}

