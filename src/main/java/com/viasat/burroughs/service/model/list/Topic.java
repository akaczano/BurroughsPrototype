package com.viasat.burroughs.service.model.list;

public class Topic {
    String name;
    int[] replicaInfo;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int[] getReplicaInfo() {
        return replicaInfo;
    }

    public void setReplicaInfo(int[] replicaInfo) {
        this.replicaInfo = replicaInfo;
    }

    @Override
    public String toString() {
        return name;
    }
}
