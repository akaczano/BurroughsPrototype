package com.viasat.burroughs.producer;

import org.apache.avro.generic.GenericRecord;

/**
 * Contains the methods that any data source must provide
 */
public interface IDataSource {
    /**
     * Checks whether this data source can be accessed
     * @return True if accessible
     */
    boolean checkAvailability();

    /**
     * Opens the data source
     */
    void open();

    /**
     * Closes the data source
     */
    void close();

    /**
     * Checks if there is more data to read
     * @return True if there is another record
     */
    boolean hasNextRecord();

    /**
     * Gets the next record (row)
     * @return A GenericRecord object containing the data
     */
    GenericRecord nextRecord();
}
