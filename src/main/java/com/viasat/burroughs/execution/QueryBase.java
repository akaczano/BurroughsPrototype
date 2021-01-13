package com.viasat.burroughs.execution;

import com.viasat.burroughs.App;
import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.command.CommandResponse;
import com.viasat.burroughs.service.model.description.*;
import com.viasat.burroughs.service.model.list.Format;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class QueryBase {

    protected final StatementService service;
    protected final QueryProperties properties;
    protected final KafkaService kafkaService;

    protected long startTime = Long.MIN_VALUE;

    private String keyConverter = "org.apache.kafka.connect.storage.StringConverter";

    public QueryBase(StatementService service, KafkaService kafkaService,
                     QueryProperties properties) {
        this.service = service;
        this.properties = properties;
        this.kafkaService = kafkaService;
    }


    public abstract void execute();

    public abstract void destroy();

    public abstract void printStatus();

    public abstract void setGroupBy(String field);

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

    public String getId() {
        return this.properties.getId();
    }

    protected String createTable(String id, String query) {
        String tableName = "burroughs_" + id;
        String statement = String.format("CREATE TABLE %s AS %s EMIT CHANGES;",
                tableName, query);
        CommandResponse response = service.executeStatement(statement, "create table");
        return tableName;
    }

    protected String createStream(String streamName, String topic, Format format) {
        return createStream(service, streamName, topic, format);
    }

    public static String createStream(StatementService service, String streamName,
                                      String topic, Format format) {
        String query = String.format("CREATE STREAM %s WITH (kafka_topic='%s', value_format='%s');",
                streamName, topic, format.toString());
        CommandResponse result = service.executeStatement(query, "create stream");
        return streamName;
    }


    protected String createConnector(String id) {
        // Sometimes we will need to do this
        // 'key.converter' = 'org.apache.kafka.connect.converters.IntegerConverter'

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
        command += "'transforms'='tombstoneHandlerExample',";
        command += "'transforms.tombstoneHandlerExample.type'='io.confluent.connect.transforms.TombstoneHandler',";
        command += "'auto.create' = true);";

        CommandResponse response = service.executeStatement(command, "create connector");
        if (response.getType().equals("error_entity")) {
            throw new ExecutionException("Failed to create connector. Make sure the output table doesn't already exist.");
        }
        return "burr_connect_" + id;
    }

    protected Map<String, DataType> GetSchema(String stream) {
        DescribeResponse description = service.executeStatement(String.format("DESCRIBE %s;", stream),
                        "describe stream");
        Map<String, DataType> results = new HashMap<>();
        for (Field f : description.getSourceDescription().getFields()) {
            results.put(f.getName(), f.getSchema().getType());
        }
        return results;
    }

    protected boolean streamExists(String streamName) {
        return streamExists(service, streamName);
    }

    public static boolean streamExists(StatementService service, String streamName) {
        ListResponse listResponse = service.executeStatement("LIST STREAMS;",
                "executed statement: LIST STREAMS");

        return Arrays.stream(listResponse.getStreams())
                .anyMatch(s -> s.getName().equalsIgnoreCase(streamName));
    }

    protected void dropStream(String streamName) {
        terminateQueries(streamName);
        drop("STREAM", streamName);
    }


    private void terminateQuery(String queryId) {
        CommandResponse result = service.executeStatement(
                String.format("TERMINATE %s;", queryId),
                "terminate query");
        if (!result.getCommandStatus().getStatus().equals("SUCCESS")) {
            throw new ExecutionException(result.getCommandStatus().getMessage());
        }
    }

    private void terminateQueries(String objectName) {
        DescribeResponse description = service.
                executeStatement(String.format("DESCRIBE %s;", objectName), "terminate queries");
        for (Query query : description.getSourceDescription().getReadQueries()) {
            terminateQuery(query.getId());
        }
        for (Query query : description.getSourceDescription().getWriteQueries()) {
            terminateQuery(query.getId());
        }
    }

    protected void dropTable(String tableName) {
        terminateQueries(tableName);
        drop("TABLE", tableName);
    }

    protected void dropConnector(String connectorName) {
        drop("CONNECTOR", connectorName);
    }

    protected void drop(String objectType, String name) {
        String command = String.format("DROP %s %s%s",
                objectType, name,
                objectType.equalsIgnoreCase("table") ? " DELETE TOPIC;" : ";");
        CommandResponse result = service.executeStatement(command, String.format("drop %s",
                objectType.toLowerCase()));
    }



    protected void printStatisticsForTable(String tableName) {
        // 1. Table description/statistics

        DescribeResponse description = service.executeStatement(
                String.format("DESCRIBE EXTENDED %s;", tableName),
                "describe table");

        if (description.getSourceDescription() == null) {
            throw new ExecutionException("There was an error retrieving query status:" +
                    " source description is null.");
        }
        String statistics = description.getSourceDescription().getStatistics();
        String[] words = statistics.split("\\s+");
        if (words.length < 4) { // This will execute if there is no data in the topic
            System.out.println("Status not available.");
            return;
        }
        System.out.println("Process rate: " + words[1] + " messages/s");
        System.out.println("Total messages processed: " + words[3]);

        // 2. Progress from kafka consumer metadata

        int tpCounter = 0;
        long currentTotal = 0;
        long maxTotal = 0;
        for (Query query : description.getSourceDescription().getWriteQueries()) {
            String consumerGroup = String.format("_confluent-ksql-default_query_%s",
                    query.getId());
            Map<TopicPartition, Long> queryStatuses = kafkaService.getCurrentOffset(consumerGroup);
            if (queryStatuses != null) {
                for (TopicPartition tp : queryStatuses.keySet()) {
                    long current = queryStatuses.get(tp);
                    long max = kafkaService.getLogMaxOffset(consumerGroup, tp);
                    System.out.printf("Query %d: %d%% (%d/%d)\n",
                            tpCounter + 1, (int) ((((double) current) / max) * 100), current, max);
                    tpCounter++;
                    currentTotal += current;
                    maxTotal += max;
                }
                double totalProgress = (((double) currentTotal) / maxTotal);
                double totalRuntime = (System.currentTimeMillis() - startTime);

                System.out.printf("Total Progress: %d%% (%d/%d)\n",
                        (int) (totalProgress * 100),
                        currentTotal, maxTotal);
                System.out.printf("Total run time: %.1f seconds\n", totalRuntime / 1000.0);
                System.out.printf("Estimated time remaining: %.1f seconds\n",
                        ((totalRuntime / (totalProgress)) - totalRuntime) / 1000.0);
            }
            else {
                System.out.println("Kafka not connected. Can't print progress information.");
            }
        }
    }
    protected void checkConnectorStatus(String connector) {
        ConnectorDescription description = service.executeStatement(
                String.format("DESCRIBE CONNECTOR %s;", connector),
                "describe connector"
        );
        ConnectorStatus status = description.getStatus();
        if (status.getTasks().length < 1) {
            System.out.println(App.ANSI_YELLOW + "Connector not running" + App.ANSI_RESET);
        }
        else {
            for (ConnectorTask task : status.getTasks()) {
                if (!task.getState().equals("RUNNING") && task.getTrace() != null) {
                    System.out.println(App.ANSI_RED + "Connector Error:" + App.ANSI_RESET);
                    System.out.println(task.getTrace());
                }
            }
        }
    }

}
