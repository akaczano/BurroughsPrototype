package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.burroughs.ConnectStatus;
import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import com.viasat.burroughs.service.model.burroughs.TableStatus;
import com.viasat.burroughs.service.model.command.CommandResponse;
import com.viasat.burroughs.service.model.description.*;
import org.apache.kafka.common.TopicPartition;


//added
import com.viasat.burroughs.execution.DebugLevels;

import java.util.*;

/**
 * Base class that provides a large array of useful methods
 * for interacting with ksqlDB
 */
public abstract class QueryBase extends QueryUtil {

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
     * group offsets during .getCommandStatus() execution
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
        super(service);
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
    public String createTable(String id, String query) {
        String tableName = "burroughs_" + id;
        String statement = String.format("CREATE TABLE %s AS %s EMIT CHANGES;",
                tableName, query);
        CommandResponse response = service.executeStatement(statement, "create table");
        DebugLevels.appendDebugLevel2("\n\t" + "createTable: " + statement);
        return tableName;
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
            DebugLevels.appendDebugLevel2("Failed to create connector using: " + response);
            throw new ExecutionException("Failed to create connector. Make sure the output table doesn't already exist.");
        }

        DebugLevels.appendDebugLevel2("\n\t" + "createConnector: " + command + "\n\t" + "Status: " + response.getCommandStatus());
        return "burr_connect_" + id;
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
