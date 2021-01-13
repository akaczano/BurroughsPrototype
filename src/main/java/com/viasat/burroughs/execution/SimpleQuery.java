package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.description.DataType;
import com.viasat.burroughs.service.model.list.Format;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.math.BigDecimal;
import java.util.*;


public class SimpleQuery extends QueryBase {

    private final SqlSelect query;

    private String stream = null;
    private String table = null;
    private String connector = null;
    private List<String> streams = new ArrayList<>();

    private String groupby;

    public SimpleQuery(StatementService service, KafkaService kafkaService,
                       QueryProperties props, SqlSelect query) {
        super(service, kafkaService, props);
        this.query = query;
    }

    public void setGroupBy(String field) {
        groupby = field;
    }

    @Override
    public void execute() {
        // TODO deal with this
        setGroupByDataType(DataType.ARRAY);

        Map<String, String> replacements = new HashMap<>();
        createStreams(replacements, query.getFrom());


        String queryString = translateQuery(query, replacements);
        System.out.print("Creating table...");
        table = createTable(properties.getId(), queryString);
        System.out.print("Done\n");
        System.out.print("Linking to database...");
        connector = createConnector(properties.getId());
        System.out.print("Done\n");
        startTime = System.currentTimeMillis();
    }

    private void createStreams(Map<String, String> replacements, SqlNode from) {
        if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin)from;
            String condition = String.format("%s %s", join.getConditionType().toString(),
                    join.getCondition().toString());
            replacements.put(condition, String.format("WITHIN %d DAYS %s",
                    Integer.MAX_VALUE, condition));
            createStreams(replacements, join.getLeft());
            createStreams(replacements, join.getRight());
        }
        else if (from instanceof SqlSelect) {
            // TODO somehow deal with subqueries
        }
        else if (from instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall)from;
            createStreams(replacements, call.operand(0));
        }
        else if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier)from;

            String streamName = String.format("burroughs_%s", identifier.toString());
            System.out.printf("Creating stream %s...", streamName);
            if (!streamExists(streamName)) {
                streams.add(createStream(streamName, identifier.getSimple().toLowerCase(), Format.AVRO));
                System.out.print("Done\n");
            }
            else {
                System.out.println("\nStream already exists");
            }
            List<String> names = new ArrayList<>();
            List<SqlParserPos> positions = new ArrayList<>();
            names.add(streamName);
            positions.add(new SqlParserPos(0, 0));
            identifier.setNames(names, positions);
        }
    }

    private String translateQuery(SqlSelect query, Map<String, String> replacements) {
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

        String preparedQuery = query.toString();
        for (String key : replacements.keySet()) {
            preparedQuery = preparedQuery.replace(key, replacements.get(key));
        }
        preparedQuery = preparedQuery.replaceAll("`", "");
        return preparedQuery;
    }

    @Override
    public void destroy() {
        if (connector != null) {
            System.out.print("Dropping connector " + connector + "...");
            dropConnector(connector);
            System.out.print("Done\n");
        }
        if (table != null) {
            System.out.print("Dropping table " + table + "...");
            dropTable(table);
            System.out.print("Done\n");
        }
        Collections.reverse(streams);
        for (String stream : streams) {
            System.out.print("Dropping stream " + stream + "...");
            dropStream(stream);
            System.out.print("Done\n");
        }
    }

    @Override
    public void printStatus() {
        if (table != null) {
            super.printStatisticsForTable(this.table);
        }
        else {
            System.out.println("Table not created");
        }
        if (connector != null) {
            super.checkConnectorStatus(connector);
        }
        else {
            System.out.println("Connector not created");
        }
    }


    @Override
    protected String createTable(String id, String query) {
        return (table = super.createTable(id, query));
    }

}
