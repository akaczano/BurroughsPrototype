package com.viasat.burroughs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jayway.jsonpath.internal.function.ParamType;
import com.viasat.burroughs.producer.Producer;
import com.viasat.burroughs.producer.ProducerEntry;
import com.viasat.burroughs.validation.QueryValidator;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.Optionality;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Unit test for simple App.
 */
public class TranslationTests extends ServiceTest{

    @Test
    public void testGroupConcatTranslation() {
        String query = "select productnum, GROUP_CONCAT(DISTINCT storer, ' ') from transactions group by productnum";
        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.BABEL)
                .build();
        try {
            SqlSelect node1 = (SqlSelect) SqlParser.create(query, config).parseQuery();
            SqlBasicCall call = (SqlBasicCall) node1.getSelectList().get(1);
            String operatorName = "collect_list";
            if (call.getFunctionQuantifier().getTypeName().getName().equalsIgnoreCase("distinct")) {
                operatorName = "collect_set";
            }
            SqlIdentifier id = call.getOperator().getNameAsId().setName(0, operatorName);
            SqlOperator op = new SqlUserDefinedFunction(id, null, null, null, new ArrayList<RelDataType>(), null);
            call.setOperator(op);
            node1.getSelectList().set(1, new SqlBasicCall(op, new SqlNode[]{call.getOperands()[0]}, call.getParserPosition()));
            System.out.println(node1.toString());
            assertNotNull(node1);
        } catch(SqlParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testJoinTranslation() throws TopicNotFoundException, UnsupportedQueryException, SqlParseException {
        String query = "select c.custid, sum(spend) " +
                "from transactions t " +
                "left join customers c " +
                "on t.basketnum = c.basketnum" +
                " left join products p " +
                "on t.productnum = p.productnum";

        String query2 = "select count(*) from (select distinct basketnum from transactions)";

        String query3 = "select * from transactions";

        SqlParser.Config config = SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.BABEL)
                .build();
        SqlSelect node1 = (SqlSelect) SqlParser.create(query, config).parseQuery();
        System.out.println(node1.getSelectList().toString());
        System.out.println(node1.getFrom().toString());

        SqlNode node2 = SqlParser.create(query2, config).parseQuery();
        SqlNode node3 = SqlParser.create(query3, config).parseQuery();
        //processFrom(node1.getFrom());

    }

    public void processFrom(SqlNode from) {
        if (from instanceof SqlJoin) {
            SqlJoin join = (SqlJoin)from;
            processFrom(join.getLeft());
            processFrom(join.getRight());
        }
        else if (from instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall)from;
            processFrom(call.operand(0));
        }
        else if (from instanceof SqlIdentifier) {
            SqlIdentifier id = (SqlIdentifier)from;
            System.out.println(id.getSimple());
            List<String> names = new ArrayList<>();
            List<SqlParserPos> positions = new ArrayList<>();
            names.add("BURROUGHS_TRANSACTIONS");
            positions.add(new SqlParserPos(0, 0));
            id.setNames(names, positions);
        }
    }

}
