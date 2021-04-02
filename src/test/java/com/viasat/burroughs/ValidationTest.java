package com.viasat.burroughs;

import com.viasat.burroughs.logging.ConsoleLogger;
import com.viasat.burroughs.logging.Logger;
import com.viasat.burroughs.service.model.list.Topic;
import com.viasat.burroughs.validation.QueryValidator;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ValidationTest extends ServiceTest{

    private final QueryValidator validator;

    public ValidationTest() {
        Logger.setLogger(new ConsoleLogger());
        validator = new QueryValidator(service);
    }

    @Before
    public void createTopics() {
        String create = "create stream valid_topic(id int) with " +
                "('kafka_topic'='valid_topic', 'value_format'='avro', 'partitions'='1');";
        service.executeStatement(create, "create fake topic");
    }

    @After
    public void deleteTopics() {
        String delete = "drop stream valid_topic delete topic;";
        service.executeStatement(delete, "delete fake topic");
    }

    @Test
    public void testInvalidTopic() {
        try {
            validator.validateQuery("SELECT SUM(SPEND), STORER FROM foo GROUP BY STORER");
            Assert.assertNotNull(null);
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e instanceof TopicNotFoundException);
        }
    }

    @Test
    public void testNoGroupBy() {
        try {
            validator.validateQuery("SELECT STORER FROM valid_topic");
            Assert.assertNotNull(null);
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            Assert.assertTrue(e instanceof UnsupportedQueryException);
        }
    }
    @Test
    public void testBadSql() {
        try {
            validator.validateQuery("SELECT SUM(SPEND) STORER FRoOM valid_topic");
            Assert.assertNotNull(null);
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            Assert.assertTrue(e instanceof SqlParseException);
        }
    }

    @Test
    public void testValidBasic() {
        try {
            validator.validateQuery("SELECT SUM(SPEND), STORER FROM valid_topic GROUP BY STORER");
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            fail();
        }
    }

    @Test
    public void testValidJoin() {
        try {
            validator.validateQuery("SELECT SUM(SPEND), STORER FROM valid_topic LEFT JOIN valid_topic GROUP BY STORER");
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            fail();
        }
    }

    @Test
    public void testGroupByInSubquery() {
        try {
            validator.validateQuery("select storer, max(total) from (select storer, basketnum, sum(spend) as total from valid_topic group by 1,2) group by 1");
            fail();
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            Assert.assertTrue(e instanceof UnsupportedQueryException);
        }
    }

    @Test
    public void testMultiCTE() {
        try {
            String query = "with transactions2 as (\n" +
                    "    select * from valid_topic\n" +
                    "), pairs as (\n" +
                    "    select\n" +
                    "        it1.productnum as source_item,\n" +
                    "        it2.productnum as target_item\n" +
                    "    from valid_topic it1\n" +
                    "    inner join transactions2 it2\n" +
                    "        on it1.basketnum = it2.basketnum\n" +
                    "        and it1.productnum != it2.productnum\n" +
                    "    where\n" +
                    "        it1.units > 0\n" +
                    "        and it2.units > 0\n" +
                    ")\n" +
                    "select \n" +
                    "    source_item,\n" +
                    "    target_item,\n" +
                    "    count(1) as frequency\n" +
                    "from pairs\n" +
                    "group by 1,2";
            validator.validateQuery(query);
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testValidateLimit() {
        try {
            validator.validateQuery("select storer, sum(spend) from valid_topic group by 1 limit 10");
        }
        catch (UnsupportedQueryException | TopicNotFoundException | SqlParseException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testValidateBadSubquery() {
        try {
            validator.validateQuery("select store, sum(spend) from (select * from valid_topic limit 10) group by 1");
            fail();
        }
        catch(UnsupportedQueryException | TopicNotFoundException | SqlParseException e) {
            Assert.assertTrue(e instanceof UnsupportedQueryException);
        }
    }

    @Test
    public void testValidateBadCTE() {
        try {
            validator.validateQuery("with things as (select * from valid_topic order by storer desc) select storer, sum(spend) from things");
            fail();
        }
        catch (UnsupportedQueryException | TopicNotFoundException | SqlParseException e) {
            Assert.assertTrue(e instanceof UnsupportedQueryException);
        }
    }

    @Test
    public void testInvalidTopicCTE() {
        try {
            validator.validateQuery("with things as (select * from foo) select storer, sum(spend) from things group by 1");
            fail();
        }
        catch(UnsupportedQueryException | TopicNotFoundException | SqlParseException e) {
            Assert.assertTrue(e instanceof TopicNotFoundException);
        }
    }

    @Test
    public void testInvalidTopicSubquery() {
        try {
            validator.validateQuery("select storer, sum(spend) from (select * from foo) group by 1");
        } catch(UnsupportedQueryException | TopicNotFoundException | SqlParseException e) {
            Assert.assertTrue(e instanceof TopicNotFoundException);
        }
    }

    @Test
    public void testForwardReference() {
        try {
            String query = "with transactions1 as (select * from valid_topic), " +
                    "transactions2 as (select * from transactions1) " +
                    "select storer, sum(spend) from transactions2 group by 1";
            validator.validateQuery(query);
        } catch(SqlParseException | UnsupportedQueryException | TopicNotFoundException e) {
            e.printStackTrace();
            fail();
        }
    }
}
