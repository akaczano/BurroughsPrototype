package com.viasat.burroughs.validation;

import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.util.Arrays;
import java.util.Collections;

public class QueryValidator {

    /**
     * KsqlDB statement service. Used to check for topic existence.
     */
    private final StatementService service;

    public QueryValidator(StatementService service) {
        this.service = service;
    }

    /**
     * Performs all query validation
     * @param query The raw query to validate
     * @return A SqlSelect object containing the parsed query
     * @throws SqlParseException Thrown when invalid SQL is encountered
     * @throws TopicNotFoundException Thrown when a topic is referenced that does not exist
     * @throws UnsupportedQueryException Thrown when an unsupported query is run (i.e. no group by)
     */
    public SqlSelect validateQuery(String query) throws SqlParseException,
            TopicNotFoundException, UnsupportedQueryException {
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.BABEL)
                .build();
        SqlNode node = SqlParser.create(query, config).parseQuery();
        if (node.isA(Collections.singleton(SqlKind.SELECT))) {
            SqlSelect selectNode = (SqlSelect) node;

            validateFrom(selectNode.getFrom());

            if (selectNode.getGroup() == null) {
                throw new UnsupportedQueryException("Query must contain a group by");
            }
            return selectNode;
        } else if (node instanceof SqlOrderBy) {
            throw new UnsupportedQueryException("Order by and limit clauses are not currently supported.");
        } else {
            throw new UnsupportedQueryException("This version of Burroughs " +
                    "only supports select statements");
        }
    }

    /**
     * Recursively traverses the from clause until the simple identifiers are found.
     * These identifiers are then checked against the topic list
     * @param from The from section of the query
     * @throws TopicNotFoundException Exception thrown when non-existent topic is referenced
     */
    private void validateFrom(SqlNode from) throws TopicNotFoundException {
        if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) from;
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            validateFrom(left);
            validateFrom(right);
        } else if (from instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) from;
            validateFrom(call.operand(0));
        } else if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            if (!validateTopic(identifier.getSimple())) {
                throw new TopicNotFoundException(from.toString());
            }
        }
    }

    /**
     * Checks if the given topic exists
     * @param topic The name of the topic
     * @return True if the topic exists
     */
    private boolean validateTopic(String topic) {
        ListResponse list = service.executeStatement("LIST TOPICS;", "list topics");
        return Arrays.stream(list.getTopics())
                .anyMatch(t -> t.getName().equalsIgnoreCase(topic));
    }
}
