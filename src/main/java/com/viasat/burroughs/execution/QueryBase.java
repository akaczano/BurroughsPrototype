package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.burroughs.ConnectStatus;
import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import com.viasat.burroughs.service.model.burroughs.TableStatus;
import com.viasat.burroughs.service.model.command.CommandResponse;
import com.viasat.burroughs.service.model.description.*;
import com.viasat.burroughs.service.model.list.Format;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Base class that provides a large array of useful methods
 * for interacting with ksqlDB
 */
public abstract class QueryBase {

    /**
     * Service object for execution ksql statements
     */
    protected final StatementService service;

    /**
     * Query properties including query id and database
     * connection info.
     */
    protected final QueryProperties properties;

    /**
     * Kafka service that can access Kafka broker directly. Used to get consumer
     * group offsets during .status execution
     */
    protected final KafkaService kafkaService;

    /**
     * Query start time
     */
    protected long startTime = Long.MIN_VALUE;

    /**
     * Key converter class used by connector. String by default.
     */
    private String keyConverter = "org.apache.kafka.connect.storage.StringConverter";


    protected List<Transform> transforms;

    /**
     * Initializes new Query given dependencies
     *
     * @param service      Statement service
     * @param kafkaService Kafka service
     * @param properties   Query properties
     */
    public QueryBase(StatementService service, KafkaService kafkaService,
                     QueryProperties properties) {
        this.service = service;
        this.properties = properties;
        this.kafkaService = kafkaService;
        transforms = new ArrayList<>();
        transforms.add(new Transform("tombstoneHandler", "io.confluent.connect.transforms.TombstoneHandler"));
    }

    /**
     * Where the query would actually be translated and executed
     */
    public abstract void execute();

    /**
     * Remove all ksqlDB objects associated with this query
     */
    public abstract void destroy();

    /**
     * Print the query status
     */
    public abstract QueryStatus getStatus();

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
     * Set the group by data type which is used to determine the Key converter class
     *
     * @param type Data type
     */
    public void setGroupByDataType(DataType type) {
        switch (type) {
            case BIGINT:
                keyConverter = "org.apache.kafka.connect.converters.LongConverter";
                return;
            case INTEGER:
                keyConverter = "org.apache.kafka.connect.converters.IntegerConverter";
                return;
            case DOUBLE:
                keyConverter = "org.apache.kafka.connect.converters.DoubleConverter";
                return;
            case BOOLEAN:
                keyConverter = "org.apache.kafka.connect.converters.BooleanConverter";
                return;
            case STRING:
                keyConverter = "org.apache.kafka.connect.storage.StringConverter";
                return;
            default:
                keyConverter = "org.apache.kafka.connect.converters.ByteArrayConverter";
        }
    }

    /**
     * Gets the query ID
     *
     * @return The query ID
     */
    public String getId() {
        return this.properties.getId();
    }

    /**
     * Creates a ksqlDB table
     *
     * @param id    The query ID to be used in the naming of the table
     * @param query The query to build the table from
     * @return The table's name
     */
    protected String createTable(String id, String query) {
        String tableName = "burroughs_" + id;
        String statement = String.format("CREATE TABLE %s AS %s EMIT CHANGES;",
                tableName, query);
        CommandResponse response = service.executeStatement(statement, "create table");
        return tableName;
    }

    protected String createStream(String name, String query) {
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
    protected String createStream(String streamName, String topic, Format format) {
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
    public static String createStream(StatementService service, String streamName,
                                      String topic, Format format) {
        String query = String.format("CREATE STREAM %s WITH (kafka_topic='%s', value_format='%s');",
                streamName, topic, format.toString());
        CommandResponse result = service.executeStatement(query, "create stream");
        return streamName;
    }
    /**
     * Version of dropStream that can be done from a static context and takes the underlying topic with it
     *
     * @param service    The statement service to send the query with
     * @param streamName The name of the stream
     * @return The name of the stream
     */
    public static CommandResponse dropStreamAndTopic(StatementService service, String streamName) {
        String query = String.format("DROP STREAM %s DELETE TOPIC;",
                streamName);
        CommandResponse result = service.executeStatement(query, "stream and topic dropped");
        return result;
    }

    /**
     * Creates a sink connector for a table
     *
     * @param id The id from which to get the table name
     * @return The name of the connector.
     */
    protected String createConnector(String id) {
        DBProvider dbInfo = properties.getDbInfo();
        String command = "CREATE SINK CONNECTOR ";
        command += "burr_connect_" + id + " WITH (";
        command += "'connector.class' = 'io.confluent.connect.jdbc.JdbcSinkConnector',";
        command += String.format("'connection.url' = 'jdbc:postgresql://%s/%s',",
                dbInfo.getConnectorDb(), dbInfo.getDatabase());
        command += String.format("'connection.user' = '%s',",
                dbInfo.getDbUser());
        command += String.format("'connection.password' = '%s',",
                dbInfo.getDbPassword());
        command += String.format("'topics' = 'BURROUGHS_%s',", id.toUpperCase());
        command += String.format("'table.name.format' = '%s',",
                dbInfo.getDbTable());
        command += "'insert.mode' = 'upsert',";
        command += "'pk.fields' = 'rowkey',";
        command += "'pk.mode' = 'record_key',";
        command += String.format("'key.converter' = '%s',", keyConverter);
        // Tombstone handler to drop null records
        // Only applies when there is a having clause
        //command += "'transforms'='tombstoneHandlerExample,serializeArray',";
        //command += "'transforms.tombstoneHandlerExample.type'='io.confluent.connect.transforms.TombstoneHandler',";
        //command += "'transforms.serializeArray.type'='com.viasat.burroughs.smt.SerializeArray$Value',";
        command += Transform.header(transforms);
        for (Transform t : this.transforms) {
            command += t.toString();
        }
        command += "'auto.create' = true);";

        CommandResponse response = service.executeStatement(command, "create connector");
        if (response.getType().equals("error_entity")) {
            throw new ExecutionException("Failed to create connector. Make sure the output table doesn't already exist.");
        }
        return "burr_connect_" + id;
    }

    /**
     * Checks if a stream exists
     *
     * @param streamName The name of the stream
     * @return Whether or not the stream exists
     */
    protected boolean streamExists(String streamName) {
        return streamExists(service, streamName);
    }

    /**
     * Checks if a stream exists
     *
     * @param service    The StatementService object to use
     * @param streamName The stream to look for
     * @return Whether or not the stream exists
     */
    public static boolean streamExists(StatementService service, String streamName) {
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
    protected void dropStream(String streamName) {
        terminateQueries(streamName);
        drop("STREAM", streamName);
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
    protected void dropTable(String tableName) {
        terminateQueries(tableName);
        drop("TABLE", tableName);
    }

    /**
     * Drops the specified connector
     *
     * @param connectorName The connector to drop
     */
    protected void dropConnector(String connectorName) {
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


    protected TableStatus getTableStatus(String tableName) {
        TableStatus status = new TableStatus();

        DescribeResponse description = service.executeStatement(
                String.format("DESCRIBE EXTENDED %s;", tableName),
                "describe table");
        if (description.getSourceDescription() == null) {
            throw new ExecutionException("There was an error retrieving query status:" +
                    " source description is null.");
        }
        String statistics = description.getSourceDescription().getStatistics();
        String[] words = statistics.split("\\s+");
        if (words.length < 4) {
            status.setHasStatus(false);
            return status;
        }
        status.setHasStatus(true);
        status.setProcessRate(Double.parseDouble(words[1]));
        status.setTotalMessages(Integer.parseInt(words[3]));
        long currentTotal = 0;
        long maxTotal = 0;
        Query[] queries = description.getSourceDescription().getWriteQueries();
        if (queries.length < 1) return status;
        Query query = queries[0];
        String consumerGroup = String.format("_confluent-ksql-default_query_%s",
                query.getId());
        List<Long> queryOffsets = new ArrayList<>();
        List<Long> queryMaxes = new ArrayList<>();
        status.setQueryOffsets(queryOffsets);
        status.setQueryMaxOffsets(queryMaxes);

        Map<TopicPartition, Long> queryStatuses = kafkaService.getCurrentOffset(consumerGroup);
        if (queryStatuses != null) {

            for (TopicPartition tp : queryStatuses.keySet()) {
                long current = queryStatuses.get(tp);
                long max = kafkaService.getLogMaxOffset(consumerGroup, tp);
                queryOffsets.add(current);
                queryMaxes.add(max);
                currentTotal += current;
                maxTotal += max;
            }
            status.setTotalProgress(currentTotal);
            status.setTotalWork(maxTotal);
            status.setTotalRuntime(System.currentTimeMillis() - startTime);
        } else {
            status.setHasStatus(false);
        }
        return status;
    }


    protected ConnectStatus getConnectorStatus(String connector) {
        ConnectStatus connectStatus = new ConnectStatus();
        ConnectorDescription description = service.executeStatement(
                String.format("DESCRIBE CONNECTOR %s;", connector),
                "describe connector"
        );
        ConnectorStatus status = description.getStatus();
        if (status == null) return connectStatus;
        if (status.getTasks().length < 1) {
            connectStatus.setConnectorRunning(false);
            return connectStatus;
        }
        connectStatus.setConnectorRunning(true);
        List<String> errors = new ArrayList<>();
        connectStatus.setErrors(errors);
        for (ConnectorTask task : status.getTasks()) {
            if (!task.getState().equals("RUNNING") && task.getTrace() != null) {
                errors.add(task.getTrace());
            }
        }
        return connectStatus;
    }

}