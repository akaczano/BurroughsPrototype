package com.viasat.burroughs;

public interface ILogger {

    void writeYellow(String text);
    void writeGreen(String text);
    void writeRed(String text);
    void write(String text);

    void writeLineYellow(String line);
    void writeLineRed(String line);
    void writeLineGreen(String line);
    void writeLine(String line);
}
