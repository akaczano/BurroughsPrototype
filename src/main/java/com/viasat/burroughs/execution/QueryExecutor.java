package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.logging.Logger;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;

import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import com.viasat.burroughs.validation.ParsedQuery;

import java.util.UUID;


public class QueryExecutor {

    /*
        Services
     */
    private final StatementService service;
    private final KafkaService kafkaService;

    /*
        Database connection info
     */
    private final DBProvider dbInfo;

    /*
        The currently executing query
     */
    private QueryBase currentQuery;

    /**
     * Creates an new QueryExecutor object
     *
     * @param service Service for sending ksql
     * @param ks      Service for querying consumer status
     * @param dbInfo  Database connection info
     */
    public QueryExecutor(StatementService service, KafkaService ks, DBProvider dbInfo) {
        this.service = service;
        this.dbInfo = dbInfo;
        this.kafkaService = ks;
    }

    /**
     * Executes a query
     *
     * @param query The query, already parsed and validated
     */
    public void executeQuery(ParsedQuery query) {
        Logger.getLogger().clearLog();
        Logger.getLogger().writeLine("executeQuery() executing query:" + '\n'
                        + query.getQuery().toString(),
                Logger.DEFAULT, Logger.LEVEL_1);
        QueryProperties props = new QueryProperties();
        props.setDbInfo(this.dbInfo);
        // Generate an ID for the query.
        props.setId(UUID.randomUUID().toString().replaceAll("-", ""));

        currentQuery = new SimpleQuery(service, kafkaService, props, query);

        try {
            currentQuery.execute();
        } catch (ExecutionException e) {
            currentQuery.destroy();
            currentQuery = null;
            throw new ExecutionException("An error occurred during query processing: " +
                    e.getMessage());
        }
        Logger.getLogger().writeLine("Your query is now active. Use .status to check on it.");
        Logger.getLogger().writeLine("Use .stop to terminate.");
    }

    /**
     * Removes all ksqlDB objects created by the query
     */
    public void stop() {
        if (currentQuery != null) {
            currentQuery.destroy();
            currentQuery = null;

        } else {
            Logger.getLogger().writeLine("No active query. Type some SQL to run one.");
        }
    }

    /**
     * Prints the status of the query if applicable
     */
    public QueryStatus status() {
        if (currentQuery == null) {
            return null;
        } else {
            QueryStatus status = currentQuery.getStatus();
            status.setQueryId(currentQuery.getId());
            return status;
        }
    }

    public boolean isExecuting() {
        return this.currentQuery != null;
    }

}
