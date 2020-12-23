package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;

import java.math.BigDecimal;
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
        // For now, we will assume this is a simple query
        String topicName = query.getFrom().toString().toLowerCase();
        String streamName = "burroughs_" + topicName;
        String id = UUID.randomUUID().toString().replaceAll("-", "");

        for (int i = 0; i < query.getGroup().getList().size(); i++) {
            SqlNode n = query.getGroup().get(i);
            if (n instanceof SqlNumericLiteral) {
                SqlNumericLiteral literal = (SqlNumericLiteral)n;
                int position = ((BigDecimal)literal.getValue()).intValueExact();
                if (literal.isInteger()) {
                    query.getGroup().set(i, query.getSelectList().get(position - 1));
                }
            }
        }
        // In the simplest case, the only thing we have to do is replace the topic
        // name with the appropriate stream name and run the query
        String preparedQuery = query.toString().replace(query.getFrom().toString(), streamName);
        preparedQuery = preparedQuery.replaceAll("`", "");

        QueryProperties props = new QueryProperties();
        props.setDbInfo(this.dbInfo);
        props.setId(id);
        props.setStreamName(streamName);
        props.setTopicName(topicName);

        currentQuery = new SimpleQuery(service, kafkaService, props, preparedQuery);
        try {
            currentQuery.execute();
        } catch(ExecutionException e) {
            currentQuery.destroy();
            throw new ExecutionException("An error occurred during query processing: " +
                    e.getMessage());
        }
        System.out.println("Your query is now active. Use .status to check on it.");
        System.out.println("Use .stop to terminate.");
        return id;
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

    private Thread t;

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
