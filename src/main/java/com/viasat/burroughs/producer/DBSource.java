package com.viasat.burroughs.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.sql.*;

/**
 * A data source that reads data from a PostgreSQL table
 */
public class DBSource implements IDataSource {

    /**
     * The JDBC postgres connection
     */
    private final Connection connection;

    /**
     * The name of the table to read from
     */
    private final String table;

    /**
     * The result of reading from the table. Populated when the data source
     * is opened.
     */
    private ResultSet resultSet;

    /**
     * The schema of the messages to build
     */
    private final Schema schema;

    /**
     * Initializes a new DBSource
     * @param connection The SQL connection to use
     * @param schema The AVRO schema to use for serialization
     * @param table The name of the database table to read from
     */
    public DBSource(Connection connection, Schema schema, String table) {
        this.connection = connection;
        this.table = table;
        this.schema = schema;
    }

    /**
     * Checks if the connection is valid
     * @return True if the DB is connected
     */
    @Override
    public boolean checkAvailability() {
        return connection != null;
    }

    /**
     * Executes a query to use as a record source
     */
    @Override
    public void open() {
        try {
            Statement stmt = connection.createStatement();
            resultSet = stmt.executeQuery(String.format("SELECT * FROM %s;", table));
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Closes the connection
     */
    @Override
    public void close() {
        try {
            connection.close();
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks if there is another record to read still
     * @return True if there is more data
     */
    @Override
    public boolean hasNextRecord() {
        try {
            return !resultSet.isLast();
        } catch(SQLException e) {
            return false;
        }
    }

    /**
     * Retrieves the next record from the result set
     * @return A GenericRecord object containing the next row
     */
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
