package com.viasat.burroughs.producer;

public class ProducerStatus {
    public static final int NOT_STARTED = 0;
    public static final int RUNNING = 1;
    public static final int PAUSED = 2;
    public static final int STOPPED = 3;
    public static final int ERROR = 4;

    private final int status;
    private final int recordsProduced;
    private final int recordsLost;
    private String errorMessage;

    public ProducerStatus(int status, int recordsProduced, int recordsLost) {
        this.status = status;
        this.recordsProduced = recordsProduced;
        this.recordsLost = recordsLost;
    }

    public int getStatus() {
        return status;
    }

    public int getRecordsProduced() {
        return recordsProduced;
    }

    public int getRecordsLost() {
        return recordsLost;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        switch (status) {
            case NOT_STARTED:
                return "Producer not started";
            case PAUSED:
                return "Paused";
            case STOPPED:
                return "Stopped";
            case ERROR:
                return "Error";
            case RUNNING:
                return "Running";
            default:
                return "UNKNOWN";
        }
    }

    public static int valueOf(String status) {
        if (status.equalsIgnoreCase("Error")) {
            return ERROR;
        } else if (status.equalsIgnoreCase("Running")) {
            return RUNNING;
        } else if (status.equalsIgnoreCase("Paused")) {
            return PAUSED;
        } else if (status.equalsIgnoreCase("Stopped")) {
            return STOPPED;
        } else {
            return NOT_STARTED;
        }
    }
}
