package com.viasat.burroughs.validation;

import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

public class QueryValidator {

    private StatementService service;

    public QueryValidator(StatementService service) {
        this.service = service;
    }

    public SqlSelect validateQuery(String query) throws SqlParseException,
            TopicNotFoundException, UnsupportedQueryException {
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.BABEL)
                .build();
        SqlNode node = SqlParser.create(query, config).parseQuery();
        if (node.isA(Collections.singleton(SqlKind.SELECT))) {
            SqlSelect selectNode = (SqlSelect)node;

            validateFrom(selectNode.getFrom());

            if (selectNode.getGroup() == null) {
                throw new UnsupportedQueryException("Query must contain a group by");
            }


            return selectNode;
        }
        else if (node instanceof SqlOrderBy) {
            throw new UnsupportedQueryException("Order by and limit clauses are not currently supported.");
        }
        else {
            throw new UnsupportedQueryException("This version of Burroughs " +
                    "only supports select statements");
        }
    }

    private void validateFrom(SqlNode from) throws TopicNotFoundException {
        if (from.isA(Collections.singleton(SqlKind.JOIN))) {
            SqlJoin join = (SqlJoin)from;
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            validateSide(left);
            validateSide(right);
        }
        else if (!validateTopic(from.toString())) {
            throw new TopicNotFoundException(from.toString());
        }
    }

    private void validateSide(SqlNode reference) throws TopicNotFoundException {
        if (reference instanceof SqlBasicCall) {
            String topicName = ((SqlBasicCall)reference).getOperands()[0].toString();
            if (!validateTopic(topicName)) {
                throw new TopicNotFoundException(topicName);
            }
        }
        else {
            if (!validateTopic(reference.toString())) {
                throw new TopicNotFoundException(reference.toString());
            }
        }
    }

    private boolean validateTopic(String topic) {
        StatementResponse response = service.executeStatement("LIST TOPICS;");
        if (response != null && !(response instanceof StatementError)) {
            ListResponse list = (ListResponse)response;
            return Arrays.stream(list.getTopics())
                    .anyMatch(t -> t.getName().equalsIgnoreCase(topic));
        }
        else {
            //TODO inform user of connection failure and abort query
            return false;
        }
    }

}
