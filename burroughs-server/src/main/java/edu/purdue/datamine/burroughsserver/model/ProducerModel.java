package edu.purdue.datamine.burroughsserver.model;

import com.viasat.burroughs.producer.ProducerStatus;

public class ProducerModel {

    private final String name;
    private final ProducerStatus status;
    private int delay = 0;

    public ProducerModel(String name, ProducerStatus status) {
        this.name = name;
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public ProducerStatus getStatus() {
        return status;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }
}
