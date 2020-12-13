package com.viasat.burroughs.execution;

import com.viasat.burroughs.DBProvider;
import com.viasat.burroughs.service.StatementService;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;

import java.util.Properties;
import java.util.UUID;

public class QueryExecutor {

    private StatementService service;
    private DBProvider dbInfo;
    private QueryBase currentQuery;
    public QueryExecutor(StatementService service, DBProvider dbInfo) {
        this.service = service;
        this.dbInfo = dbInfo;
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
                int position = literal.getPrec();
                if (literal.isInteger()) {
                    query.getGroup().set(i, query.getSelectList().get(position - 1));
                }
            }
        }
        // In the simplest case, the only thing we have to do is replace the topic
        // name with the appropriate stream name and run the query
        String preparedQuery = query.toString().replace(query.getFrom().toString(), streamName);
        preparedQuery = preparedQuery.replaceAll("`", "");


        currentQuery = new SimpleQuery(service, packageProperties()
                , preparedQuery, topicName, streamName);
        currentQuery.execute(id);
        return id;
    }

    private Properties packageProperties() {
        Properties props = new Properties();
        props.put("DB_HOST", dbInfo.getDbHost());
        props.put("DATABASE", dbInfo.getDatabase());
        props.put("DB_USER", dbInfo.getDbUser());
        props.put("DB_PASSWORD", dbInfo.getDbPassword());
        props.put("DB_TABLE", dbInfo.getDbTable());
        return props;
    }

    public void stop() {
        currentQuery.destroy();
    }

}
