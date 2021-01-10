package com.viasat.burroughs.producer;

import org.apache.avro.generic.GenericRecord;


public interface IDataSource {
    boolean checkAvailability();
    void open();
    void close();
    boolean hasNextRecord();
    GenericRecord nextRecord();
}
