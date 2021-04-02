package com.viasat.burroughs.logging;


import com.viasat.burroughs.logging.ILogger;

public class ConsoleLogger implements ILogger {

    // Character sequences used to change text color
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_RESET = "\u001B[0m";

    @Override
    public void writeYellow(String text) {
        System.out.print(ANSI_YELLOW + text + ANSI_RESET);
    }

    @Override
    public void writeGreen(String text) {
        System.out.print(ANSI_GREEN + text + ANSI_RESET);
    }

    @Override
    public void writeRed(String text) {
        System.out.print(ANSI_RED + text + ANSI_RESET);
    }

    @Override
    public void write(String text) {
        System.out.print(text);
    }

    @Override
    public void writeLineYellow(String line) {
        System.out.println(ANSI_YELLOW + line + ANSI_RESET);
    }

    @Override
    public void writeLineRed(String line) {
        System.out.println(ANSI_RED + line + ANSI_RESET);
    }

    @Override
    public void writeLineGreen(String line) {
        System.out.println(ANSI_GREEN + line + ANSI_RESET);
    }

    @Override
    public void writeLine(String line) {
        System.out.println(line);
    }
}
