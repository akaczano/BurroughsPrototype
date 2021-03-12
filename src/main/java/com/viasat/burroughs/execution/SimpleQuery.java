package com.viasat.burroughs.execution;

import com.viasat.burroughs.Logger;
import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import com.viasat.burroughs.service.model.description.DataType;
import com.viasat.burroughs.service.model.description.DescribeResponse;
import com.viasat.burroughs.service.model.list.Format;
import com.viasat.burroughs.service.model.description.Field;
import com.viasat.burroughs.validation.ParsedQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import java.math.BigDecimal;
import java.util.*;


public class SimpleQuery extends QueryBase {

    // The parsed query
    private final ParsedQuery parsedQuery;

    // The ksqlDB table. There can only be one of these per query
    private String table = null;

    // The ksqlDB sink connector
    private String connector = null;

    // All of the associated streams
    private final List<StreamEntry> streams = new ArrayList<>();

    private Stack<String> names = new Stack<>();

    private int subqueryCounter = 0;

    /**
     * Creates a new query object
     * @param service The ksqlDB statement service
     * @param kafkaService The Kafka service for query status
     * @param props Query properties
     * @param query The SQL query itself
     */
    public SimpleQuery(StatementService service, KafkaService kafkaService,
                       QueryProperties props, ParsedQuery query) {
        super(service, kafkaService, props);
        this.parsedQuery = query;
    }

    /**
     * Translates and executes the query
     */
    @Override
    public void execute() {

        SqlSelect query = parsedQuery.getQuery();
        SqlNodeList extraStreams = parsedQuery.getWithList();
        if (extraStreams != null) {
            for (SqlNode n : extraStreams) {
                SqlWithItem withItem = (SqlWithItem) n;
                SqlSelect select = (SqlSelect) withItem.query;
                Map<String, String> replacements = new HashMap<>();
                List<SqlBasicCall> extras = new ArrayList<>();
                createStreams(replacements, select.getFrom(), extras);
                String queryText = translateQuery(select, replacements, extras);
                String name = createStream("burroughs_" + withItem.name.getSimple(), queryText);
                streams.add(new StreamEntry(name, true));
            }
        }

        if (parsedQuery.getLimit() > 0) {
            Transform limitTransform = new Transform("limit", "com.viasat.burroughs.smt.Limit");
            limitTransform.addProperty("limit", Integer.toString(parsedQuery.getLimit()));
            transforms.add(limitTransform);
        }

        // Stores translations as a mapping of text to replacement
        Map<String, String> replacements = new HashMap<>();
        List<SqlBasicCall> extras = new ArrayList<>();
        createStreams(replacements, query.getFrom(), extras);

        // Create table and connector
        String queryString = translateQuery(query, replacements, extras);
        Logger.getLogger().write("Creating table...");
        table = createTable(properties.getId(), queryString);
        Logger.getLogger().write("Done\n");
        Logger.getLogger().write("Linking to database...");
        setGroupByDataType(determineDataType(table));
        addKeyTransforms(query);
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
    private void createStreams(Map<String, String> replacements, SqlNode from, List<SqlBasicCall> extras) {
        if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin)from;
            translateCondition(join.getCondition(), extras);

            String condition = String.format("%s %s", join.getConditionType().toString(),
                    join.getCondition().toString());
            replacements.put(condition, String.format("WITHIN %d DAYS %s",
                    Integer.MAX_VALUE, condition));
            createStreams(replacements, join.getLeft(), extras);
            createStreams(replacements, join.getRight(), extras);
        }
        else if (from instanceof SqlSelect) {
            SqlSelect subquery = (SqlSelect)from;
            Map<String, String> sqReplacements = new HashMap<>();
            List<SqlBasicCall> sqExtras = new ArrayList<>();
            createStreams(sqReplacements, subquery.getFrom(), sqExtras);
            String queryText = translateQuery(subquery, sqReplacements, sqExtras);
            if (names.isEmpty()) {
                names.push(getId() + "_" + subqueryCounter++);
            }
            String stream = createStream(names.pop(), queryText);
            replacements.put("(" + subquery.toString() + ")", stream);
            streams.add(new StreamEntry(stream, true));
        }
        else if (from instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall)from;
            if (call.getOperator().toString().equalsIgnoreCase("AS") && call.operand(0) instanceof SqlSelect) {
                names.push(call.operand(1).toString());
            }
            createStreams(replacements, call.operand(0), extras);
        }
        else if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier)from;

            String streamName = String.format("burroughs_%s", identifier.toString());
            Logger.getLogger().write(String.format("Creating stream %s...", streamName));
            if (!streamExists(streamName)) {
                streams.add(new StreamEntry(createStream(streamName, identifier.getSimple().toLowerCase(), Format.AVRO), false));
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
    private String translateQuery(SqlSelect query, Map<String, String> replacements, List<SqlBasicCall> extras) {
        if (query.getGroup() != null) {
            for (int i = 0; i < query.getGroup().getList().size(); i++) {
                SqlNode n = query.getGroup().get(i);
                if (n instanceof SqlNumericLiteral) {
                    SqlNumericLiteral literal = (SqlNumericLiteral) n;
                    int position = ((BigDecimal) literal.getValue()).intValueExact();
                    if (literal.isInteger()) {
                        SqlNode field = query.getSelectList().get(position - 1);
                        if (field instanceof SqlBasicCall) {
                            field = ((SqlBasicCall) field).operand(0);
                        }
                        query.getGroup().set(i, field);
                    }
                }
            }
        }
        if (extras.size() > 0) {
            SqlBinaryOperator andOperator = new SqlBinaryOperator("AND", SqlKind.AND, 24, false, null, null, null);
            SqlBasicCall base = extras.get(0);
            for (int i = 1; i < extras.size(); i++) {
                base = new SqlBasicCall(andOperator,
                        new SqlNode[]{base, extras.get(i)}, base.getParserPosition());
            }
            if (query.getWhere() == null) {
                query.setWhere(base);
            }
            else {
                SqlBasicCall where = (SqlBasicCall) query.getWhere();
                SqlBasicCall compoundWhere = new SqlBasicCall(andOperator,
                        new SqlNode[]{where, base}, where.getParserPosition());
                query.setWhere(compoundWhere);
            }
        }

        for (int i = 0; i < query.getSelectList().size(); i++) {
            SqlNode newNode = translateFunction(query.getSelectList().get(i));
            query.getSelectList().set(i, newNode);
        }


        String preparedQuery = query.toString();

        if (query.getFrom() instanceof SqlJoin && query.getGroup() == null) {
            preparedQuery += " PARTITION BY NULL";
        }

        for (String key : replacements.keySet()) {
            preparedQuery = preparedQuery.replace(key, replacements.get(key));
        }
        preparedQuery = preparedQuery.replaceAll("`", "");
        return preparedQuery;
    }

    private void translateCondition(SqlNode condition, List<SqlBasicCall> rejects) {
        if (condition instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) condition;
            if (call.getOperator().getName().equalsIgnoreCase("AND")) {
                SqlBasicCall firstCondition = call.operand(0);
                SqlBasicCall secondCondition = call.operand(1);
                call.setOperator(firstCondition.getOperator());
                call.setOperand(0, firstCondition.operand(0));
                call.setOperand(1, firstCondition.operand(1));
                rejects.add(secondCondition);
                translateCondition(call, rejects);
            }
        }
    }

    private SqlNode translateFunction(SqlNode item) {
        if (item instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall)item;
            if (call.getOperator().toString().equalsIgnoreCase("AS")) {
                call.setOperand(0, translateFunction(call.operand(0)));
                return call;
            }
            else if (call.getOperator().toString().equalsIgnoreCase("GROUP_CONCAT")) {
                String operatorName = "COLLECT_LIST";
                if (call.getFunctionQuantifier() != null &&call.getFunctionQuantifier().getValue().toString()
                        .equalsIgnoreCase("DISTINCT")) {
                    operatorName = "COLLECT_SET";
                }
                SqlIdentifier id = call.getOperator().getNameAsId().setName(0, operatorName);

                SqlOperator op = new SqlUserDefinedFunction(id, null, null, null, new ArrayList<RelDataType>(), null);
                if (this.transforms.stream().noneMatch(t -> t.name().equals("serializeArray"))) {
                    Transform arraySerializer = new Transform("serializeArray", "com.viasat.burroughs.smt.SerializeArray$Value");
                    if (call.getOperandList().size() > 1) {
                        String separator = call.operand(1).toString();
                        separator = separator.substring(1, separator.length() - 1);
                        arraySerializer.addProperty("separator", separator);
                    }
                    transforms.add(arraySerializer);
                }
                return new SqlBasicCall(op, new SqlNode[]{call.getOperands()[0]}, call.getParserPosition());
            }
            else if (call.getOperator().toString().equalsIgnoreCase("CAST")) {

            }
        }
        return item;
    }

    private void addKeyTransforms(SqlSelect query) {
        Transform keyTransform = new Transform("IncludeKey", "com.viasat.burroughs.smt.IncludeKey");
        transforms.add(keyTransform);
        if (query.getGroup().size() < 2) {
            keyTransform.addProperty("field_name", query.getGroup()
                    .toString().replace("`", ""));
            keyTransform.addProperty("multiple", "false");
            return;
        }
        keyTransform.addProperty("multiple", "true");

        StringBuilder groupParam = new StringBuilder();

        for (SqlNode n : query.getGroup()) {
            if (n instanceof SqlIdentifier) {
                if (groupParam.length() > 0) {
                    groupParam.append(",");
                }
                SqlIdentifier id = (SqlIdentifier)n;
                String type = getDataType(id, query.getFrom());
                groupParam.append(id.names.get(id.names.size() - 1));
                groupParam.append(":");
                groupParam.append(type);
            }
            else {
                // Function call
            }
        }
        keyTransform.addProperty("field_name", groupParam.toString());
    }

    private String getDataType(SqlIdentifier id, SqlNode from) {
        if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            return getDataTypeFromStream(identifier.getSimple(), id.names.get(id.names.size()-1));
        }
        else if (from instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall)from;
            if (id.names.size() < 2 || id.names.get(0).equalsIgnoreCase(call.operand(1).toString())) {
                return getDataType(id, call.operand(0));
            }
        }
        else if (from instanceof SqlJoin) {
            String result1 = getDataType(id, ((SqlJoin) from).getLeft());
            if (result1 != null) return result1;
            return getDataType(id, ((SqlJoin) from).getRight());
        }
        else if (from instanceof SqlSelect) {
            return null;
        }
        return null;
    }

    private String getDataTypeFromStream(String stream, String field) {
        Map<String, DataType> schema = GetSchema(stream);
        if (schema.containsKey(field)) {
            DataType type = schema.get(field);
            if (type == DataType.INTEGER) {
                return "INT";
            }
            else if (type == DataType.DOUBLE) {
                return "DOUBLE";
            }
            else {
                return "STRING";
            }
        }
        return null;
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
        for (StreamEntry stream : streams) {
            Logger.getLogger().write("Dropping stream " + stream.getStreamName() + "...");
            terminateQueries(stream.getStreamName());
            if (stream.isDeleteTopic()) {
                dropStreamAndTopic(service, stream.getStreamName());
            }
            else {
                dropStream(stream.getStreamName());
            }
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
     * Stores the table name whenever a table is creaated
     * @param id The query ID to be used in the naming of the table
     * @param query The query to build the table from
     * @return The table name
     */
    @Override
    protected String createTable(String id, String query) {
        return (table = super.createTable(id, query));
    }

}
