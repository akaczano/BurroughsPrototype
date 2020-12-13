package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.body.StreamProperties;
import com.viasat.burroughs.service.model.command.CommandResponse;
import com.viasat.burroughs.service.model.description.DescribeResponse;
import com.viasat.burroughs.service.model.description.Query;
import com.viasat.burroughs.service.model.list.Format;
import com.viasat.burroughs.service.model.list.ListResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

public abstract class QueryBase {

    protected final StatementService service;
    private final Properties dbProps;

    public QueryBase(StatementService service, Properties dbProps) {
        this.service = service;
        this.dbProps = dbProps;
    }

    public abstract void execute(String id);

    public abstract void destroy();

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
        String command = "CREATE SINK CONNECTOR ";
        command += "burr_connect_" + id + " WITH (";
        command += "'connector.class' = 'io.confluent.connect.jdbc.JdbcSinkConnector',";
        command += String.format("'connection.url' = 'jdbc:postgresql://%s/%s',",
                dbProps.getProperty("DB_HOST"), dbProps.getProperty("DATABASE"));
        command += String.format("'connection.user' = '%s',",
                dbProps.getProperty("DB_USER"));
        command += String.format("'connection.password' = '%s',",
                dbProps.getProperty("DB_PASSWORD"));
        command += String.format("'topics' = 'BURROUGHS_%s',", id.toUpperCase());
        command += String.format("'table.name.format' = '%s',",
                dbProps.getProperty("DB_TABLE"));
        command += "'insert.mode' = 'upsert',";
        command += "'pk.fields' = 'rowkey',";
        command += "'pk.mode' = 'record_key',";
        command += "'auto.create' = true);";

        StatementResponse response = service.executeStatement(command);
        if (response == null) {
            throw new ExecutionException("Failed to create connector due to connection error.");
        }
        else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError)response);
        }
        else {
            CommandResponse result = (CommandResponse)response;
            return "burr_connect_" + id;
        }
    }

    protected boolean streamExists(String streamName) {
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
            CommandResponse result = (CommandResponse)response;
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
            CommandResponse result = (CommandResponse)dropResponse;
        }
    }

    protected void dropOutput() {
        String conString = String.format("jdbc:postgresql://%s/%s",
                dbProps.getProperty("DB_HOST"), dbProps.getProperty("DATABASE"));
        Properties props = new Properties();
        props.put("user", dbProps.get("DB_USER"));
        props.put("password", dbProps.get("DB_PASSWORD"));
        try {
            Connection conn = DriverManager.getConnection(conString, props);
            conn.createStatement().execute(String.format("DROP TABLE %s;",
                    dbProps.getProperty("DB_TABLE")));
        } catch(SQLException e) {
            throw new ExecutionException("Failed to drop table from database");
        }
    }

}
