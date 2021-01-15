package com.viasat.burroughs.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Scanner;

/**
 * A data source that reads from a delimited text file
 */
public class FileSource implements IDataSource {

    /**
     * The file to read from
     */
    private final File file;

    /**
     * The schema to use in serializing the data
     */
    private final Schema schema;

    /**
     * Scanner object used to read the file
     */
    private Scanner scanner;

    /**
     * Whether or not the file has a header which should be skipped
     */
    private boolean hasHeader = false;

    /**
     * The delimiter that separates the fields
     */
    private String delimiter = ",";

    public FileSource(File file, Schema schema) {
        this.file = file;
        this.schema = schema;
    }

    /**
     * Checks if the file can be read fromm
     * @return True if the file exists
     */
    public boolean checkAvailability() {
        return file != null && file.exists();
    }

    /**
     * Opens the file and skips the header if necessary
     */
    public void open() {
        try {
            scanner = new Scanner(this.file);
            if (hasHeader) {
                scanner.nextLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Closes the file
     */
    public void close() {
        if (scanner != null) {
            scanner.close();
        }
    }

    /**
     * Checks if there is more data in the file
     * @return True if there is another line
     */
    public boolean hasNextRecord() {
        return scanner.hasNextLine();
    }

    /**
     * Reads the next line in the file, parses it, and formats it according to the
     * provides schema.
     * @return
     */
    public GenericRecord nextRecord() {
        String line = scanner.nextLine();
        String[] values = line.split(delimiter);

        GenericRecord record = new GenericData.Record(this.schema);
        for (int i = 0; i < schema.getFields().size(); i++) {
            String value = values[i].trim();
            Schema.Field field = schema.getFields().get(i);
            String type = field.schema().getType().getName();

            if (type.equalsIgnoreCase("string"))
                record.put(field.name(), value);
            else if (type.equalsIgnoreCase("int")) {
                Object lt = field.getObjectProp("logicalType");
                if (lt != null && lt.equals("date")) {
                    record.put(field.name(), (int)(Date.parse(value) / 86_400_000L));
                }
                else {
                    record.put(field.name(), Integer.parseInt(value));
                }
            }
            else if (type.equalsIgnoreCase("boolean"))
                record.put(field.name(), Boolean.parseBoolean(value));
            else if (type.equalsIgnoreCase("long"))
                record.put(field.name(), Long.parseLong(value));
            else if (type.equalsIgnoreCase("double"))
                record.put(field.name(), Double.parseDouble(value));
            else
                throw new ProducerException(String.format("Unsupported type: %s",
                        field.schema().getType().getName()));
        }
        return record;
    }

    public void setHasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }
}
