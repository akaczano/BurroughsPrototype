package com.viasat.burroughs.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.sql.*;

public class DBSource implements IDataSource {

    private final Connection connection;
    private final String table;
    private ResultSet resultSet;
    private Schema schema;

    public DBSource(Connection connection, Schema schema, String table) {
        this.connection = connection;
        this.table = table;
        this.schema = schema;
    }

    @Override
    public boolean checkAvailability() {
        return connection != null;
    }

    @Override
    public void open() {
        try {
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(String.format("SELECT * FROM %s;", table));
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean hasNextRecord() {
        try {
            return !resultSet.isLast();
        } catch(SQLException e) {
            return false;
        }
    }

    @Override
    public GenericRecord nextRecord() {
        try {
            resultSet.next();
            GenericRecord record = new GenericData.Record(this.schema);
            for (int i = 0; i < schema.getFields().size(); i++) {
                Schema.Field field = schema.getFields().get(i);
                String type = field.schema().getType().getName();

                if (type.equalsIgnoreCase("string"))
                    record.put(field.name(), resultSet.getString(i + 1));
                else if (type.equalsIgnoreCase("int")) {
                    Object lt = field.getObjectProp("logicalType");
                    if (lt != null && lt.equals("date")) {
                        record.put(field.name(), (int)(resultSet.getDate(i + i).getDate() / 86_400_000L));
                    }
                    else {
                        record.put(field.name(), resultSet.getInt(i + 1));
                    }
                }
                else if (type.equalsIgnoreCase("boolean"))
                    record.put(field.name(), resultSet.getBoolean(i + 1));
                else if (type.equalsIgnoreCase("long"))
                    record.put(field.name(), resultSet.getLong(i + 1));
                else if (type.equalsIgnoreCase("double"))
                    record.put(field.name(), resultSet.getDouble(i + 1));
                else
                    throw new ProducerException(String.format("Unsupported type: %s",
                            field.schema().getType().getName()));
            }
            return record;
        } catch(SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
