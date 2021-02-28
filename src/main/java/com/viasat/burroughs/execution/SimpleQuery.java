package com.viasat.burroughs.execution;

import com.viasat.burroughs.Logger;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import com.viasat.burroughs.service.model.description.DataType;
import com.viasat.burroughs.service.model.list.Format;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

//added
import com.viasat.burroughs.execution.DebugLevels;


import java.math.BigDecimal;
import java.util.*;


public class SimpleQuery extends QueryBase {

    // The parsed query
    private final SqlSelect query;

    // The ksqlDB table. There can only be one of these per query
    private String table = null;

    // The ksqlDB sink connector
    private String connector = null;

    // All of the associated streams
    private final List<String> streams = new ArrayList<>();

    // Not currently in use
    private String groupby;

    /**
     * Creates a new query object
     * @param service The ksqlDB statement service
     * @param kafkaService The Kafka service for query status
     * @param props Query properties
     * @param query The SQL query itself
     */
    public SimpleQuery(StatementService service, KafkaService kafkaService,
                       QueryProperties props, SqlSelect query) {
        super(service, kafkaService, props);
        this.query = query;
    }

    /**
     * Sets the group by field from which the key converter can be determined
     * @param field Field name
     */
    public void setGroupBy(String field) {
        groupby = field;
    }

    /**
     * Translates and executes the query
     */
    @Override
    public void execute() {
        // TODO: deal with this
        setGroupByDataType(DataType.ARRAY);

        // Stores translations as a mapping of text to replacement
        Map<String, String> replacements = new HashMap<>();
        createStreams(replacements, query.getFrom());

        // Create table and connector
        String queryString = translateQuery(query, replacements);
        Logger.getLogger().write("Creating table...");
        table = createTable(properties.getId(), queryString);
        Logger.getLogger().write("Done\n");
        Logger.getLogger().write("Linking to database...");
        connector = createConnector(properties.getId());
        Logger.getLogger().write("Done\n");
        startTime = System.currentTimeMillis();
    }

    /**
     * Creates all of the streams a query depends on and discovers
     * necessary translations
     * @param replacements
     * @param from
     */
    private void createStreams(Map<String, String> replacements, SqlNode from) {

	DebugLevels.appendDebugLevels("createStreams inputs: "+ replacements + " and "+ from);

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

	    DebugLevels.appendDebugLevels("createStreams: interpreting " + from + " as SqlSelect.");

            // TODO somehow deal with subqueries
        }
        else if (from instanceof SqlBasicCall) {
	    DebugLevels.appendDebugLevels("createStreams: interpreting " + from + " as SqlBasicCall.");


            SqlBasicCall call = (SqlBasicCall)from;
            createStreams(replacements, call.operand(0));
        }
        else if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier)from;

            String streamName = String.format("burroughs_%s", identifier.toString());
            Logger.getLogger().write(String.format("Creating stream %s...", streamName));
            if (!streamExists(streamName)) {
                streams.add(createStream(streamName, identifier.getSimple().toLowerCase(), Format.AVRO));
                Logger.getLogger().write("Done\n");
            }
            else {
                Logger.getLogger().writeLine("\nStream already exists");
            }
            List<String> names = new ArrayList<>();
            List<SqlParserPos> positions = new ArrayList<>();
            names.add(streamName);
            positions.add(new SqlParserPos(0, 0));
            identifier.setNames(names, positions);
        }
    }

    /**
     * Translates the query by doing the following steps
     * 1. Replace integers in the group by with the actual stream name
     * 2. Perform all replacements demanded by createStreams
     * 3. Remove backticks
     * @param query The parsed query
     * @param replacements The replacements to perform
     * @return The fully translated query string to pass to a create table statement
     */
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

        for (int i = 0; i < query.getSelectList().size(); i++) {
            SqlNode item = query.getSelectList().get(i);
            if (item instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall)item;
                if (call.getOperator().toString().equalsIgnoreCase("GROUP_CONCAT")) {
                    String operatorName = "COLLECT_LIST";
                    if (call.getFunctionQuantifier() != null &&call.getFunctionQuantifier().getValue().toString()
                            .equalsIgnoreCase("DISTINCT")) {
                        operatorName = "COLLECT_SET";
                    }
                    SqlIdentifier id = call.getOperator().getNameAsId().setName(0, operatorName);

                    SqlOperator op = new SqlUserDefinedFunction(id, null, null, null, new ArrayList<RelDataType>(), null);
                    query.getSelectList().set(i, new SqlBasicCall(op, new SqlNode[]{call.getOperands()[0]}, call.getParserPosition()));
                    if (this.transforms.stream().noneMatch(t -> t.name().equals("serializeArray"))) {
                        Transform arraySerializer = new Transform("serializeArray", "com.viasat.burroughs.smt.SerializeArray$Value");
                        if (call.getOperandList().size() > 1) {
                            String separator = call.operand(1).toString();
                            separator = separator.substring(1, separator.length() - 1);
                            arraySerializer.addProperty("separator", separator);
                        }
                        transforms.add(arraySerializer);
                    }
                }
            }
        }

	DebugLevels.appendDebugLevel2("translateQuery (loop 2) generated " + query);


        String preparedQuery = query.toString();
        for (String key : replacements.keySet()) {
            preparedQuery = preparedQuery.replace(key, replacements.get(key));
        }
        preparedQuery = preparedQuery.replaceAll("`", "");

	DebugLevels.appendDebugLevel2("translateQuery (loop 3) generated: " + preparedQuery);


        return preparedQuery;
    }

    /**
     * Removes all associated ksqlDB objects.
     */
    @Override
    public void destroy() {
        if (connector != null) {
            Logger.getLogger().write("Dropping connector " + connector + "...");
            dropConnector(connector);
            Logger.getLogger().write("Done\n");
        }
        if (table != null) {
            Logger.getLogger().write("Dropping table " + table + "...");
            dropTable(table);
            Logger.getLogger().write("Done\n");
        }
        Collections.reverse(streams); // Delete streams in the reverse order they were created
        for (String stream : streams) {
            Logger.getLogger().write("Dropping stream " + stream + "...");
            dropStream(stream);
            Logger.getLogger().write("Done\n");
        }
    }

    /**
     * Print table statistics and connector status
     */
    @Override
    public QueryStatus getStatus() {

        QueryStatus status = new QueryStatus();
        if (table != null) {
            status.setTableStatus(this.getTableStatus(this.table));
        }
        if (connector != null) {
            status.setConnectorStatus(this.getConnectorStatus(this.connector));
        }
        return status;
    }

    /**
     * Stores the table name whenever a table is created
     * @param id The query ID to be used in the naming of the table
     * @param query The query to build the table from
     * @return The table name
     */
    @Override
    protected String createTable(String id, String query) {
        return (table = super.createTable(id, query));
    }

}
