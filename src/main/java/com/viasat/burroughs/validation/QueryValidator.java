package com.viasat.burroughs.validation;

import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.list.ListResponse;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

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
            if (selectNode.getFrom().isA(Collections.singleton(SqlKind.JOIN))) {
                throw new UnsupportedQueryException("Joins are not supported at this time");
            }
            if (!validateTopic(selectNode.getFrom().toString())) {
                throw new TopicNotFoundException(selectNode.getFrom().toString());
            }
            if (selectNode.getGroup() == null) {
                throw new UnsupportedQueryException("Query must contain a group by");
            }
            return selectNode;
        }
        else {
            throw new UnsupportedQueryException("This version of Burroughs " +
                    "only supports select statements");
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
