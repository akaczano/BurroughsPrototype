package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;

import org.apache.calcite.sql.SqlSelect;

import java.util.UUID;

public class QueryExecutor {

    private final StatementService service;
    private final KafkaService kafkaService;
    private final DBProvider dbInfo;
    private QueryBase currentQuery;
    public QueryExecutor(StatementService service, KafkaService ks, DBProvider dbInfo) {
        this.service = service;
        this.dbInfo = dbInfo;
        this.kafkaService = ks;
    }

    public String executeQuery(SqlSelect query) {
        QueryProperties props = new QueryProperties();
        props.setDbInfo(this.dbInfo);
        props.setId(UUID.randomUUID().toString().replaceAll("-", ""));

        currentQuery = new SimpleQuery(service, kafkaService, props, query);
        if (query.getGroup().getList().size() == 1) {
            String groupByField = query.getGroup().get(0).toString();
            currentQuery.setGroupBy(groupByField);
        }
        try {
            currentQuery.execute();
        } catch(ExecutionException e) {
            currentQuery.destroy();
            currentQuery = null;
            throw new ExecutionException("An error occurred during query processing: " +
                    e.getMessage());
        }
        System.out.println("Your query is now active. Use .status to check on it.");
        System.out.println("Use .stop to terminate.");
        return props.getId();
    }

    public void stop() {
        if (currentQuery != null) {
            currentQuery.destroy();
            currentQuery = null;
        }
        else {
            System.out.println("No active query. Type some SQL to run one.");
        }
    }

    public void status() {
        if (currentQuery == null) {
            System.out.println("There is no active query. Enter some SQL to execute one.");
        }
        else {
            System.out.printf("Active Query ID: %s\n", currentQuery.getId());
            currentQuery.printStatus();
        }
    }


}
