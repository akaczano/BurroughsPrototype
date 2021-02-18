package com.viasat.burroughs;

import com.viasat.burroughs.validation.QueryValidator;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ValidationTest extends ServiceTest{

    private final QueryValidator validator;

    public ValidationTest() {
        validator = new QueryValidator(service);
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
            validator.validateQuery("SELECT STORER FROM transactions");
            Assert.assertNotNull(null);
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e instanceof UnsupportedQueryException);
        }
    }
    @Test
    public void testBadSql() {
        try {
            validator.validateQuery("SELECT SUM(SPEND) STORER FRoOM transactions");
            Assert.assertNotNull(null);
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e instanceof SqlParseException);
        }
    }

    @Test
    public void testValidBasic() {
        try {
            validator.validateQuery("SELECT SUM(SPEND), STORER FROM transactions GROUP BY STORER");
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            System.out.println(e.getMessage());
            fail();
        }
    }

    @Test
    public void testValidJoin() {
        try {
            validator.validateQuery("SELECT SUM(SPEND), STORER FROM transactions LEFT JOIN transactions  GROUP BY STORER");
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            fail();
        }
    }

    @Test
    public void testGroupByInSubquery() {
        try {
            validator.validateQuery("select storer, max(total) from (select storer, basketnum, sum(spend) as total from transactions group by 1,2) group by 1");
            fail();
        } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
            Assert.assertTrue(e instanceof UnsupportedQueryException);
        }
    }

    @Test
    public void testMultiCTE() {
        try {
            String query = "with transactions2 as (\n" +
                    "    select * from transactions\n" +
                    "), pairs as (\n" +
                    "    select\n" +
                    "        it1.productnum as source_item,\n" +
                    "        it2.productnum as target_item\n" +
                    "    from transactions it1\n" +
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
            validator.validateQuery("select storer, sum(spend) from transactions group by 1 limit 10");
        }
        catch (UnsupportedQueryException | TopicNotFoundException | SqlParseException e) {
            fail();
        }
    }

}
