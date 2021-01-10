package com.viasat.burroughs.producer;

import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class FileSource implements IDataSource {

    private final File file;
    private final Schema schema;
    private Scanner scanner;
    private boolean hasHeader = false;
    private String delimiter = ",";

    public FileSource(File file, Schema schema) {
        this.file = file;
        this.schema = schema;
    }

    public boolean checkAvailability() {
        return file != null && file.exists();
    }

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

    public void close() {
        if (scanner != null) {
            scanner.close();
        }
    }

    public boolean hasNextRecord() {
        return scanner.hasNextLine();
    }

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
            else if (type.equalsIgnoreCase("int"))
                record.put(field.name(), Integer.parseInt(value));
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
