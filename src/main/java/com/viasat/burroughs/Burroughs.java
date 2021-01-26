package com.viasat.burroughs;

import com.viasat.burroughs.execution.ExecutionException;
import com.viasat.burroughs.execution.QueryBase;
import com.viasat.burroughs.execution.QueryExecutor;
import com.viasat.burroughs.producer.ProducerInterface;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.StatusService;
import com.viasat.burroughs.service.model.HealthStatus;
import com.viasat.burroughs.service.model.burroughs.BurroughsConnection;
import com.viasat.burroughs.service.model.burroughs.QueryStatus;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class Burroughs implements DBProvider {


    // System configuration
    private String ksqlHost;
    private String dbHost;
    private String database;
    private String dbUser;
    private String dbPassword;
    private String dbTable;
    private String kafkaHost;
    private String schemaRegistry;
    private String connectorDB;

    /**
     * Provides access to ksqlDB
     */
    private StatementService service;

    /**
     * In charge of the parsing and execution of queries
     */
    private QueryExecutor executor;

    /**
     * Used to get metadata directly from Kafka
     */
    private KafkaService kafkaService;

    /**
     * Loads, stores, and executes producers
     */
    private ProducerInterface producerInterface;

    // Connection status
    private boolean ksqlConnected = false;
    private boolean dbConnected = false;


    /**
     * Burroughs constructor. Initialize command handlers.
     */
    public Burroughs() {
    }

    public ProducerInterface producerInterface() {
        return this.producerInterface;
    }

    /**
     * Called after the configuration is loaded from environment variables.
     * Established connection to KsqlDB, PostgreSQL and the Kafka broker. Also
     * loads producers.
     */
    public void init() {
        ksqlConnected = checkKsqlConnection();
        dbConnected = checkDatabaseConnection() != null;
        this.service = new StatementService(ksqlHost);
        this.kafkaService = new KafkaService(kafkaHost);
        this.executor = new QueryExecutor(service, kafkaService, this);
        if (ksqlConnected) {
            this.producerInterface = new ProducerInterface(kafkaHost, schemaRegistry, this);
        }
    }

    /**
     * Called when Burroughs exits. Stops any running producer threads.
     */
    public void dispose() {
        if (producerInterface != null) {
            producerInterface.stopProducers();
        }
    }


    /**
     * Method that processes all SQL queries
     *
     * @param query Raw SQL to execute
     */
    public void processQuery(String query) {
        // Surprisingly, Calcite will not tolerate a semicolon at the end of the query
        if (query.endsWith(";")) {
            query = query.substring(0, query.length() - 1);
        }
        // Perform validation (and parsing also)
        SqlSelect parsedQuery = validateQuery(query);
        if (parsedQuery != null) {
            if (this.dbTable == null) {
                System.out.println("No output table set. Use .table to configure one.");
            } else {
                // Execute query
                executor.executeQuery(parsedQuery);
            }
        }
    }


    public BurroughsConnection connection() {
        BurroughsConnection conn = new BurroughsConnection();
        conn.setKsqlHost(ksqlHost);
        conn.setdBHost(dbHost);
        conn.setKsqlConnected(ksqlConnected);
        conn.setDbConnected(dbConnected);
        return conn;
    }


    /**
     * Destroys all Burroughs objects
     */
    public void stop(boolean keepTable) {
        executor.stop();
        // Check if keep-table flag is present
        // If not, drop the table
        if (!keepTable) {

            System.out.print("Dropping output table...");
            String conString = String.format("jdbc:postgresql://%s/%s",
                    dbHost, database);
            Properties props = new Properties();
            props.put("user", dbUser);
            props.put("password", dbPassword);
            try {
                Connection conn = DriverManager.getConnection(conString, props);
                conn.createStatement().execute(String.format("DROP TABLE IF EXISTS %s;",
                        dbTable));
            } catch (SQLException e) {
                throw new ExecutionException("Failed to drop table from database");
            }
            System.out.print("Done\n");
        }
    }



    public Field[] topic(String topicName) {
        if (!QueryBase.streamExists(service, "BURROUGHS_" + topicName)) {
            QueryBase.createStream(service, "BURROUGHS_" + topicName, topicName,
                    Format.AVRO);
        }
        DescribeResponse description = service.executeStatement("DESCRIBE BURROUGHS_" + topicName + ";",
                "retrieve topic metadata");
        return description.getSourceDescription().getFields();
    }



    public Topic[] topics() {
        ListResponse results = service.executeStatement("SHOW TOPICS;",
                "list topics");
        return results.getTopics();
    }


    public QueryStatus queryStatus() {
        return this.executor.status();
    }


    /**
     * Verifies that KsqlDB is connected
     *
     * @return Whether or not a connection is established
     */
    private boolean checkKsqlConnection() {
        StatusService statusService = new StatusService(ksqlHost);

        // Send request to /healthcheck endpoint
        HealthStatus status = statusService.checkConnection();
        if (status == null) {
            System.err.printf("Failed to connect to ksqlDB at %s\n", ksqlHost);
            return false;
        } else if (!status.isHealthy()) {
            System.out.printf("%sksqlDB server is unhealthy%s", BurroughsCLI.ANSI_YELLOW, BurroughsCLI.ANSI_RESET);
            return false;
        } else {
            System.out.println("ksqlDB connected successfully");
            return true;
        }
    }

    /**
     * Verifies the configured PostgreSQL connection
     * @return A Connection object or null if it failed
     */
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
            // If the exception message says that the database doesn't exist,
            // create the database and then reconnect
            if (e.getMessage().contains("database \"") && e.getMessage().contains("\" does not exist")) {
                try {
                    String cString = String.format("jdbc:postgresql://%s/", dbHost);
                    Connection c = DriverManager.getConnection(cString, props);
                    Statement createDB = c.createStatement();
                    createDB.execute(String.format("CREATE DATABASE %s;", database));
                    return checkDatabaseConnection();
                } catch (SQLException ex) {
                    e = ex;
                }
            }
            System.err.printf("Failed to connect to database: %s\n", e.getMessage());
            System.err.printf("Database host: %s\nUser: %s\n Database: %s\n", dbHost, dbUser, database);
        }
        return conn;
    }

    /**
     * Performs basic query validation. Checks that the SQL is valid,
     * the topics reference exist, and that the query is supported by
     * Burroughs.
     * @param query The raw query string
     * @return The parsed query as a SqlSelect object
     */
    private SqlSelect validateQuery(String query) {
        QueryValidator validator = new QueryValidator(this.service);
        try {
            return validator.validateQuery(query);
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            System.out.printf("%sValidation error: %s%s\n",
                    BurroughsCLI.ANSI_RED, e.getMessage(), BurroughsCLI.ANSI_RESET);
            return null;
        }
    }


    // Getters and setters
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

    public void setDbTable(String table) { this.dbTable = table; }

    public void setKafkaHost(String kafkaHost) {
        this.kafkaHost = kafkaHost;
    }

    public String getConnectorDb() {
        return connectorDB;
    }

    public void setConnectorDb(String connectorDB) {
        this.connectorDB = connectorDB;
    }

    public void setSchemaRegistry(String schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

}
