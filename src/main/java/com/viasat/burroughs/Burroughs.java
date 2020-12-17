package com.viasat.burroughs;

import com.viasat.burroughs.execution.ExecutionException;
import com.viasat.burroughs.execution.QueryBase;
import com.viasat.burroughs.execution.QueryExecutor;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.StatusService;
import com.viasat.burroughs.service.model.HealthStatus;
import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.description.DescribeResponse;
import com.viasat.burroughs.service.model.description.Field;
import com.viasat.burroughs.service.model.list.Format;
import com.viasat.burroughs.service.model.list.ListResponse;
import com.viasat.burroughs.service.model.list.Topic;
import com.viasat.burroughs.validation.QueryValidator;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jline.reader.UserInterruptException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Burroughs implements DBProvider {

    private String ksqlHost;
    private String dbHost;
    private String database;
    private String dbUser;
    private String dbPassword;
    private String dbTable;

    private StatementService service;
    private QueryExecutor executor;

    private final Map<String, CommandHandler> handlers;

    public Burroughs() {
        this.handlers = new HashMap<>();
        this.handlers.put(".stop", this::handleStop);
        this.handlers.put(".table", this::handleTable);
        this.handlers.put(".topics", this::handleTopics);
        this.handlers.put(".status", this::handleStatus);
        this.handlers.put(".topic", this::handleTopic);
    }

    public void init() {
        checkKsqlConnection();
        checkDatabaseConnection();
        this.service = new StatementService(ksqlHost);
        this.executor = new QueryExecutor(service, this);
    }

    public void handleCommand(String command) {
        if (command == null) return;
        try {
            command = command.trim();
            if (command.startsWith(".")) {
                String commandWord = command.split("\\s+")[0];
                if (this.handlers.containsKey(commandWord)) {
                    this.handlers.get(commandWord).handle(command);
                } else {
                    System.out.println("Unknown command: " + commandWord);
                }
            } else {
                processQuery(command);
            }
        } catch(ExecutionException e) {
            System.out.println(e.getMessage());
        }
    }

    private void processQuery(String query) {
        if (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1);
        }
        SqlSelect parsedQuery = validateQuery(query);
        if (parsedQuery != null) {
            if (this.dbTable == null) {
                System.out.println("No output table set. Use .table to configure one.");
            }
            else {
                executor.executeQuery(parsedQuery);
            }
        }
    }

    private void handleStop(String command) {
        executor.stop();
    }

    private void handleTable(String command) {
        String[] words = command.split("\\s+");

        if (words.length == 1) {
            if (dbTable == null) {
                System.out.println("No table selected yet.");
            }
            else {
                System.out.println(this.dbTable);
            }
        }
        else if (words.length > 2) {
            System.out.println("Usage: .table <tablename>");
        }
        else {
            this.dbTable = words[1];
            System.out.println("Set output table to " + words[1]);
        }
    }

    private void handleTopic(String command) {
        String[] words = command.split("\\s+");
        if (words.length != 2) {
            System.out.println("Usage: .topic <topic>");
        }
        else {
            String topicName = words[1];
            if (!QueryBase.streamExists(service, "BURROUGHS_" + topicName)) {
                QueryBase.createStream(service, "BURROUGHS_" + topicName, topicName,
                        Format.AVRO);
            }
            StatementResponse response = service.executeStatement("DESCRIBE BURROUGHS_" + topicName + ";");
            if (response == null) {
                System.out.println("Failed to retrieve topic metadata due to connection error.");
            }
            else if (response instanceof StatementError) {
                throw new ExecutionException((StatementError)response);
            }
            else {
                DescribeResponse description = (DescribeResponse)response;
                System.out.println("Field Name: Type");
                for (Field f : description.getSourceDescription().getFields()) {
                    System.out.printf("%s: %s\n", f.getName(), f.getSchema().getType());
                }
            }
        }
    }

    private void handleTopics(String command) {
        StatementResponse response = service.executeStatement("SHOW TOPICS;");
        if (response == null) {
            throw new ExecutionException("Failed to list topics due to connection error.");
        }
        else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError)response);
        }
        else {
            ListResponse results = (ListResponse) response;
            for (Topic t : results.getTopics()) {
                System.out.println(t);
            }
        }
    }

    private void handleStatus(String command) {
        System.out.println("Status:");
        this.executor.status();
    }

    private boolean checkKsqlConnection() {
        StatusService statusService = new StatusService(ksqlHost);

        // Verify ksqlDB connection
        HealthStatus status = statusService.checkConnection();
        if (status == null) {
            System.err.printf("Failed to connect to ksqlDB at %s\n", ksqlHost);
            return false;
        }
        else if  (!status.isHealthy()) {
            System.out.printf("%sksqlDB server is unhealthy%s", App.ANSI_YELLOW, App.ANSI_RESET);
            return false;
        }
        else {
            System.out.println("ksqlDB connected successfully");
            return true;
        }
    }

    private Connection checkDatabaseConnection() {
        String conString = String.format("jdbc:postgresql://%s/%s", dbHost, database);
        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPassword);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(conString, props);
            System.out.println("Database connected successfully");
        } catch (SQLException e) {
            System.err.printf("Failed to connect to database: %s\n", e.getMessage());
            System.err.printf("Database host: %s\nUser: %s\n Database: %s\n", dbHost, dbUser, database);
        }
        return conn;
    }

    private  SqlSelect validateQuery(String query) {
        QueryValidator validator = new QueryValidator(this.service);
        try {
            return validator.validateQuery(query);
        } catch(SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            System.out.printf("%sValidation error: %s%s\n",
                    App.ANSI_RED, e.getMessage(), App.ANSI_RESET);
            return null;
        }
    }

    public String getKsqlHost() {
        return ksqlHost;
    }

    public void setKsqlHost(String ksqlHost) {
        this.ksqlHost = ksqlHost;
    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getDbTable() {
        return this.dbTable;
    }
}
