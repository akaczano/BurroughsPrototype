package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.Logger;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;

import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import org.apache.calcite.sql.SqlSelect;

import java.util.UUID;


//added
import com.viasat.burroughs.execution.DebugLevels;


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
     * @param service Service for sending ksql
     * @param ks Service for querying consumer status
     * @param dbInfo Database connection info
     */
    public QueryExecutor(StatementService service, KafkaService ks, DBProvider dbInfo) {
        this.service = service;
        this.dbInfo = dbInfo;
        this.kafkaService = ks;
    }

    /**
     * Executes a query
     * @param query The query, already parsed and validated
     */
    public void executeQuery(SqlSelect query) {

	DebugLevels.appendDebugLevel("Executing Query:" + '\n' + query.toString());  //added

        QueryProperties props = new QueryProperties();
        props.setDbInfo(this.dbInfo);
        // Generate an ID for the query.
        props.setId(UUID.randomUUID().toString().replaceAll("-", ""));

        // Create a new SimpleQuery object
        // Originally, I intended to have a handful of different query classes,
        // each one handling a different kind of query, but I now doubt the sense
        // of that design
        currentQuery = new SimpleQuery(service, kafkaService, props, query);
        if (query.getGroup().getList().size() == 1) {
            // Set the group by field to correctly configure the connector
            // Currently, this isn't working and all keys are being converted to
            // hex strings
            String groupByField = query.getGroup().get(0).toString();
            currentQuery.setGroupBy(groupByField);
        }
        try {
            // Show time
	    //DebugLevels.appendDebugLevel(currentQuery.toString());
            currentQuery.execute();
        } catch(ExecutionException e) {
            currentQuery.destroy();
            currentQuery = null;
            throw new ExecutionException("An error occurred during query processing: " +
                    e.getMessage());
        }
        Logger.getLogger().writeLine("Your query is now active. Use .status to check on it.");
        Logger.getLogger().writeLine("Use .stop to terminate.");
        DebugLevels.debugLevel2+= "The query that is currently being executed is "+ currentQuery;
              

    }

    /**
     * Removes all ksqlDB objects created by the query
     */
    public void stop() {
        if (currentQuery != null) {
            currentQuery.destroy();
            currentQuery = null;

	    DebugLevels.clearDebugLevels();  //added to clear debug traceback
        }
        else {
            Logger.getLogger().writeLine("No active query. Type some SQL to run one.");
        }
    }

    /**
     * Prints the status of the query if applicable
     */
    public QueryStatus status() {
        if (currentQuery == null) {
            return null;
        }
        else {
            QueryStatus status = currentQuery.getStatus();
            status.setQueryId(currentQuery.getId());
            return status;
        }
    }


}
