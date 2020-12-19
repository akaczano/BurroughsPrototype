package com.viasat.burroughs.execution;

import com.viasat.burroughs.App;
import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.body.StreamProperties;
import com.viasat.burroughs.service.model.command.CommandResponse;
import com.viasat.burroughs.service.model.description.DescribeResponse;
import com.viasat.burroughs.service.model.description.Query;
import com.viasat.burroughs.service.model.list.Format;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.apache.kafka.common.TopicPartition;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public abstract class QueryBase {

    protected final StatementService service;
    protected final QueryProperties properties;
    protected final KafkaService kafkaService;

    protected long startTime = Long.MIN_VALUE;

    public QueryBase(StatementService service, KafkaService kafkaService,
                     QueryProperties properties) {
        this.service = service;
        this.properties = properties;
        this.kafkaService = kafkaService;
    }


    public abstract void execute();

    public abstract void destroy();

    public abstract void printStatus();

    public String getId() {
        return this.properties.getId();
    }

    protected String createTable(String id, String query) {
        String tableName = "burroughs_" + id;
        String statement = String.format("CREATE TABLE %s AS %s EMIT CHANGES;",
                tableName, query);
        StatementResponse response = service.executeStatement(statement,
                new StreamProperties(true));
        if (response == null) {
            throw new ExecutionException("Failed to create table due to connection error");
        } else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError) response);
        } else {
            CommandResponse result = (CommandResponse) response;
        }
        return tableName;
    }

    protected String createStream(String streamName, String topic, Format format) {
        return createStream(service, streamName, topic, format);
    }

    public static String createStream(StatementService service, String streamName,
                                      String topic, Format format) {
        String query = String.format("CREATE STREAM %s WITH (kafka_topic='%s', value_format='%s');",
                streamName, topic, format.toString());
        StatementResponse response = service.executeStatement(query);
        if (response == null) {
            throw new ExecutionException("Failed to create stream due to connection error");
        } else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError) response);
        } else {
            CommandResponse result = (CommandResponse) response;
        }
        return streamName;
    }


    protected String createConnector(String id) {
        DBProvider dbInfo = properties.getDbInfo();
        String command = "CREATE SINK CONNECTOR ";
        command += "burr_connect_" + id + " WITH (";
        command += "'connector.class' = 'io.confluent.connect.jdbc.JdbcSinkConnector',";
        command += String.format("'connection.url' = 'jdbc:postgresql://%s/%s',",
                dbInfo.getDbHost(), dbInfo.getDatabase());
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
        command += "'auto.create' = true);";

        StatementResponse response = service.executeStatement(command);
        if (response == null) {
            throw new ExecutionException("Failed to create connector due to connection error.");
        } else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError) response);
        } else {
            CommandResponse result = (CommandResponse) response;
            return "burr_connect_" + id;
        }
    }

    protected boolean streamExists(String streamName) {
        return streamExists(service, streamName);
    }

    public static boolean streamExists(StatementService service, String streamName) {
        StatementResponse response = service.executeStatement("LIST STREAMS;");
        if (!(response instanceof ListResponse)) {
            throw new ExecutionException("Failed to executed statement: LIST STREAMS;");
        }
        ListResponse listResponse = (ListResponse) response;
        return Arrays.stream(listResponse.getStreams())
                .anyMatch(s -> s.getName().equalsIgnoreCase(streamName));
    }

    protected void dropStream(String streamName) {
        terminateQueries(streamName);
        drop("STREAM", streamName);
    }


    private void terminateQuery(String queryId) {
        StatementResponse response = service.executeStatement(
                String.format("TERMINATE %s;", queryId));
        if (response == null) {
            throw new ExecutionException("Failed to terminate query due to connection error");
        } else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError) response);
        } else {
            CommandResponse result = (CommandResponse) response;
            if (!result.getCommandStatus().getStatus().equals("SUCCESS")) {
                throw new ExecutionException(result.getCommandStatus().getMessage());
            }
        }
    }

    private void terminateQueries(String objectName) {
        StatementResponse response = service.
                executeStatement(String.format("DESCRIBE %s;", objectName));
        if (response == null) {
            throw new ExecutionException("Failed to terminate queries due to connection error.");
        } else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError) response);
        } else {
            DescribeResponse description = (DescribeResponse) response;
            for (Query query : description.getSourceDescription().getReadQueries()) {
                terminateQuery(query.getId());
            }
            for (Query query : description.getSourceDescription().getWriteQueries()) {
                terminateQuery(query.getId());
            }
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
        StatementResponse dropResponse = service.executeStatement(String.format(
                "DROP %s %s;", objectType, name));
        if (dropResponse == null) {
            throw new ExecutionException(String
                    .format("Failed to drop %s due to connection error",
                            objectType.toLowerCase()));
        } else if (dropResponse instanceof StatementError) {
            throw new ExecutionException((StatementError) dropResponse);
        } else {
            CommandResponse result = (CommandResponse) dropResponse;
        }
    }

    protected void dropOutput() {
        DBProvider dbInfo = properties.getDbInfo();
        String conString = String.format("jdbc:postgresql://%s/%s",
                dbInfo.getDbHost(), dbInfo.getDatabase());
        Properties props = new Properties();
        props.put("user", dbInfo.getDbUser());
        props.put("password", dbInfo.getDbPassword());
        try {
            Connection conn = DriverManager.getConnection(conString, props);
            conn.createStatement().execute(String.format("DROP TABLE IF EXISTS %s;",
                    dbInfo.getDbTable()));
        } catch (SQLException e) {
            throw new ExecutionException("Failed to drop table from database");
        }
    }

    protected void printStatisticsForTable(String tableName) {
        StatementResponse response = service
                .executeStatement(String.format("DESCRIBE EXTENDED %s;", tableName));
        if (response == null) {
            throw new ExecutionException("Failed to retrieve query status due to connection error.");
        } else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError) response);
        } else {
            DescribeResponse description = (DescribeResponse) response;
            if (description.getSourceDescription() == null) {
                throw new ExecutionException("There was an error retrieving query status:" +
                        " source description is null.");
            }
            String statistics = description.getSourceDescription().getStatistics();
            String[] words = statistics.split("\\s+");
            System.out.println("Process rate: " + words[1] + " messages/s");
            System.out.println("Total messages processed: " + words[3]);
            KafkaService kafkaService = new KafkaService("broker:29092");
            int tpCounter = 0;
            long currentTotal = 0;
            long maxTotal = 0;
            for (Query query : description.getSourceDescription().getWriteQueries()) {
                String consumerGroup = String.format("_confluent-ksql-default_query_%s",
                        query.getId());
                Map<TopicPartition, Long> queryStatuses = kafkaService.getCurrentOffset(consumerGroup);
                for (TopicPartition tp : queryStatuses.keySet()) {
                    long current = queryStatuses.get(tp);
                    long max = kafkaService.getLogMaxOffset(consumerGroup, tp);
                    System.out.printf("Query %d: %d%% (%d/%d)\n",
                            tpCounter + 1, (int) ((((double) current) / max) * 100), current, max);
                    tpCounter++;
                    currentTotal += current;
                    maxTotal += max;
                }
            }
            double totalProgress = (((double) currentTotal) / maxTotal);
            double totalRuntime = (System.currentTimeMillis() - startTime);

            System.out.printf("Total Progress: %d%% (%d/%d)\n",
                    (int)(totalProgress * 100),
                    currentTotal, maxTotal);
            System.out.printf("Total run time: %.1f seconds\n", totalRuntime / 1000.0);
            System.out.printf("Estimated time remaining: %.1f seconds\n",
                    ((totalRuntime / (totalProgress)) - totalRuntime)/1000.0);
        }

    }

}
