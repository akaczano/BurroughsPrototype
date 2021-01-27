package com.viasat.burroughs.service.model.burroughs;

import java.util.List;

public class TableStatus {
    private boolean hasStatus;
    private double processRate;
    private int totalMessages;
    private long totalRuntime;
    private long totalProgress;
    private long totalWork;

    private List<Long> queryOffsets;
    private List<Long> queryMaxOffsets;

    public double getProcessRate() {
        return processRate;
    }

    public void setProcessRate(double processRate) {
        this.processRate = processRate;
    }

    public int getTotalMessages() {
        return totalMessages;
    }

    public void setTotalMessages(int totalMessages) {
        this.totalMessages = totalMessages;
    }

    public long getTotalRuntime() {
        return totalRuntime;
    }

    public void setTotalRuntime(long totalRuntime) {
        this.totalRuntime = totalRuntime;
    }

    public List<Long> getQueryOffsets() {
        return queryOffsets;
    }

    public void setQueryOffsets(List<Long> queryOffsets) {
        this.queryOffsets = queryOffsets;
    }

    public List<Long> getQueryMaxOffsets() {
        return queryMaxOffsets;
    }

    public void setQueryMaxOffsets(List<Long> queryMaxOffsets) {
        this.queryMaxOffsets = queryMaxOffsets;
    }

    public void setHasStatus(boolean hasStatus) {
        this.hasStatus = hasStatus;
    }

    public boolean hasStatus() {
        return this.hasStatus;
    }

    public long getTotalProgress() {
        return totalProgress;
    }

    public void setTotalProgress(long totalProgress) {
        this.totalProgress = totalProgress;
    }

    public long getTotalWork() {
        return totalWork;
    }

    public void setTotalWork(long totalWork) {
        this.totalWork = totalWork;
    }
}
