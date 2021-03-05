package com.viasat.burroughs.validation;

import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

public class QueryValidator {

    /**
     * KsqlDB statement service. Used to check for topic existence.
     */
    private final StatementService service;

    private SqlNodeList withList;

    public QueryValidator(StatementService service) {
        this.service = service;
    }

    /**
     * Performs all query validation
     *
     * @param query The raw query to validate
     * @return A SqlSelect object containing the parsed query
     * @throws SqlParseException         Thrown when invalid SQL is encountered
     * @throws TopicNotFoundException    Thrown when a topic is referenced that does not exist
     * @throws UnsupportedQueryException Thrown when an unsupported query is run (i.e. no group by)
     */
    public ParsedQuery validateQuery(String query) throws SqlParseException,
            TopicNotFoundException, UnsupportedQueryException {
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.BABEL)
                .build();
        SqlNode node = SqlParser.create(query, config).parseQuery();

        int limit = 0;
        SqlSelect selectNode;

        if (node instanceof SqlWith) {
            SqlWith with = (SqlWith) node;
            withList = with.withList;
            for (int i = 0; i < withList.size(); i++) {
                SqlNode item =  ((SqlWithItem)withList.get(i)).query;
                if (item instanceof SqlOrderBy) {
                    throw new UnsupportedQueryException("Order by and limit not allowed in subquery.");
                }
                SqlSelect select = (SqlSelect)item;
                validateSubquery(select);
                validateFrom(select.getFrom());
            }
            selectNode = (SqlSelect) with.body;
        } else if (node instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) node;
            if (orderBy.fetch instanceof SqlNumericLiteral) {
                SqlNumericLiteral literal = (SqlNumericLiteral)orderBy.fetch;
                limit = ((BigDecimal) literal.getValue()).intValueExact();
            }
            else {
                throw new UnsupportedQueryException("Invalid limit");
            }
            if (orderBy.orderList.size() > 0) {
                throw new UnsupportedQueryException("Query cannot contain order by.");
            }
            selectNode = (SqlSelect) orderBy.query;
        } else if (node instanceof SqlSelect) {
            selectNode = (SqlSelect) node;
        } else {
            throw new UnsupportedQueryException("This version of Burroughs " +
                    "only supports select statements");
        }

        validateFrom(selectNode.getFrom());

        if (selectNode.getGroup() == null) {
            throw new UnsupportedQueryException("Query must contain a group by");
        }
        return new ParsedQuery(selectNode, withList, limit);
    }

    /**
     * Recursively traverses the from clause until the simple identifiers are found.
     * These identifiers are then checked against the topic list
     *
     * @param from The from section of the query
     * @throws TopicNotFoundException Exception thrown when non-existent topic is referenced
     */
    private void validateFrom(SqlNode from) throws TopicNotFoundException, UnsupportedQueryException {
        if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) from;
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            validateFrom(left);
            validateFrom(right);
            SqlBasicCall condition = (SqlBasicCall)join.getCondition();
                validateCondition(condition);
        } else if (from instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) from;
            validateFrom(call.operand(0));
        } else if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            if (withList != null) {
                for (int i = 0; i < withList.size(); i++) {
                    SqlWithItem withItem = (SqlWithItem) withList.get(i);
                    if (withItem.name.toString().equalsIgnoreCase(identifier.toString())) {
                        return;
                    }
                }
            }
            if (!validateTopic(identifier.getSimple())) {
                throw new TopicNotFoundException(from.toString());
            }
        } else if (from instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) from;
            validateSubquery(select);
            validateFrom(select.getFrom());
        } else if (from instanceof SqlOrderBy) {
            throw new UnsupportedQueryException("Subquery cannot have limit or order by");
        }
    }

    private void validateSubquery(SqlSelect select) throws UnsupportedQueryException {
        if (select.getGroup() != null) {
            throw new UnsupportedQueryException("Subquery cannot contain group by");
        }
        for (SqlNode n : select.getSelectList()) {
            if (n.isA(SqlKind.AGGREGATE)) {
                throw new UnsupportedQueryException("Subquery cannot contain aggregate function.");
            }
        }

    }

    private void validateCondition(SqlNode condition) throws UnsupportedQueryException {
        if (condition instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall)condition;
            if (call.getOperator().getName().equalsIgnoreCase("OR")) {
                throw new UnsupportedQueryException("On condition cannot contain OR");
            }
            validateCondition(call.operand(0));
            validateCondition(call.operand(1));
        }
    }

    /**
     * Checks if the given topic exists
     *
     * @param topic The name of the topic
     * @return True if the topic exists
     */
    private boolean validateTopic(String topic) {
        ListResponse list = service.executeStatement("LIST TOPICS;", "list topics");
        return Arrays.stream(list.getTopics())
                .anyMatch(t -> t.getName().equalsIgnoreCase(topic));
    }
}
