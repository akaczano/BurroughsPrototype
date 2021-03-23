package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.command.CommandResponse;
import com.viasat.burroughs.service.model.description.DataType;
import com.viasat.burroughs.service.model.description.DescribeResponse;
import com.viasat.burroughs.service.model.description.Field;
import com.viasat.burroughs.service.model.description.Query;
import com.viasat.burroughs.service.model.list.Format;
import com.viasat.burroughs.service.model.list.ListResponse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class QueryUtil {

    private final StatementService service;

    public QueryUtil(StatementService service) {
        this.service = service;
    }

    public DataType determineDataType(String table) {
        DescribeResponse res =  service.executeStatement("DESCRIBE " + table + ";", "describe table");
        //String key = res.getSourceDescription().getKey();
        Field[] fields = res.getSourceDescription().getFields();
        DataType type = DataType.ARRAY;
        for (Field f : fields) {
            if (f.getType() != null && f.getType().equalsIgnoreCase("KEY")) {
                type = f.getSchema().getType();
                break;
            }
        }
        return type;
    }

    /**
     * Gets the schema, as a field name - data type table,
     * for a stream
     *
     * @param stream The desired stream
     * @return The schema
     */
    public Map<String, DataType> GetSchema(String stream) {
        DescribeResponse description = service.executeStatement(String.format("DESCRIBE %s;", stream),
                "describe stream");
        Map<String, DataType> results = new HashMap<>();
        for (Field f : description.getSourceDescription().getFields()) {
            results.put(f.getName(), f.getSchema().getType());
        }
        return results;
    }


    public String createStream(String name, String query) {
        String ksql = String.format("CREATE STREAM %s as %s EMIT CHANGES;", name, query);
        CommandResponse result = service.executeStatement(ksql, "create stream");
        return name;
    }

    /**
     * Creates a ksqlDB stream
     *
     * @param streamName The name of the stream to create
     * @param topic      The topic to create the stream from
     * @param format     The topic serialization format (only AVRO for now)
     * @return The stream name
     */
    public String createStream(String streamName, String topic, Format format) {
        return createStream(service, streamName, topic, format);
    }

    /**
     * Actually does the work of creating the stream.
     *
     * @param service    The statement service to send the query with
     * @param streamName The name of the stream
     * @param topic      The topic to create the stream from
     * @param format     The value format
     * @return The name of the stream
     */
    public String createStream(StatementService service, String streamName,
                                      String topic, Format format) {
        String query = String.format("CREATE STREAM %s WITH (kafka_topic='%s', value_format='%s');",
                streamName, topic, format.toString());
        CommandResponse result = service.executeStatement(query, "create stream");
        return streamName;
    }


    /**
     * Checks if a stream exists
     *
     * @param streamName The stream to look for
     * @return Whether or not the stream exists
     */
    public boolean streamExists(String streamName) {
        ListResponse listResponse = service.executeStatement("LIST STREAMS;",
                "executed statement: LIST STREAMS");

        return Arrays.stream(listResponse.getStreams())
                .anyMatch(s -> s.getName().equalsIgnoreCase(streamName));
    }

    /**
     * Utility method for dropping a stream
     *
     * @param streamName The stream to drop
     */
    public void dropStream(String streamName) {
        terminateQueries(streamName);
        drop("STREAM", streamName);
    }

    /**
     * Version of dropStream that can be done from a static context and takes the underlying topic with it
     *
     * @param streamName The name of the stream
     * @return The name of the stream
     */
    public CommandResponse dropStreamAndTopic(String streamName) {
        terminateQueries(streamName);
        String query = String.format("DROP STREAM %s DELETE TOPIC;",
                streamName);
        CommandResponse result = service.executeStatement(query, "drop stream and topic");
        return result;
    }

    /**
     * Terminates the given query
     *
     * @param queryId The query to terminate
     */
    private void terminateQuery(String queryId) {
        CommandResponse result = service.executeStatement(
                String.format("TERMINATE %s;", queryId),
                "terminate query");
        if (!result.getCommandStatus().getStatus().equals("SUCCESS")) {
            throw new ExecutionException(result.getCommandStatus().getMessage());
        }
    }

    /**
     * Terminates all queries that depend upon the given object
     *
     * @param objectName The object to check, usually a table
     */
    protected void terminateQueries(String objectName) {
        DescribeResponse description = service.
                executeStatement(String.format("DESCRIBE %s;", objectName), "terminate queries");
        for (Query query : description.getSourceDescription().getReadQueries()) {
            terminateQuery(query.getId());
        }
        for (Query query : description.getSourceDescription().getWriteQueries()) {
            terminateQuery(query.getId());
        }
    }

    /**
     * Drops the specified table
     *
     * @param tableName the table to drop
     */
    public void dropTable(String tableName) {
        terminateQueries(tableName);
        drop("TABLE", tableName);
    }

    /**
     * Drops the specified connector
     *
     * @param connectorName The connector to drop
     */
    public void dropConnector(String connectorName) {
        drop("CONNECTOR", connectorName);
    }

    /**
     * Generalized drop method which can drop streams, tables, and connectors.
     * It also deletes the underlying topic for any tables.
     *
     * @param objectType The type of object (stream, table, or connector)
     * @param name       The name of the object
     */
    protected void drop(String objectType, String name) {
        String command = String.format("DROP %s %s%s",
                objectType, name,
                objectType.equalsIgnoreCase("table") ? " DELETE TOPIC;" : ";");
        CommandResponse result = service.executeStatement(command, String.format("drop %s",
                objectType.toLowerCase()));
    }

}
