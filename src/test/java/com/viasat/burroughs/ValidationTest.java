package com.viasat.burroughs;

import com.viasat.burroughs.validation.QueryValidator;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ValidationTest extends ServiceTest{

    @Test
    public void testValidation(){
        QueryValidator validator = new QueryValidator(service);
        try {
            validator.validateQuery("SELECT SUM(SPEND), STORER FROM foo GROUP BY STORER");
            Assert.assertNotNull(null);
        } catch(SqlParseException | TopicNotFoundException  | UnsupportedQueryException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e instanceof TopicNotFoundException);
        }

        try {
            validator.validateQuery("SELECT STORER FROM transactions");
            Assert.assertNotNull(null);
        } catch(SqlParseException | TopicNotFoundException  | UnsupportedQueryException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e instanceof UnsupportedQueryException);
        }

        try {
            validator.validateQuery("SELECT SUM(SPEND) STORER FRoOM transactions");
            Assert.assertNotNull(null);
        } catch(SqlParseException | TopicNotFoundException  | UnsupportedQueryException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e instanceof SqlParseException);
        }

        try {
            validator.validateQuery("SELECT SUM(SPEND), STORER FROM transactions GROUP BY STORER");
        } catch(SqlParseException | TopicNotFoundException  | UnsupportedQueryException e) {
            System.out.println(e.getMessage());
            fail();
        }

        try {
            validator.validateQuery("SELECT SUM(SPEND), STORER FROM transactions LEFT JOIN transactions  GROUP BY STORER");
        } catch(SqlParseException | TopicNotFoundException  | UnsupportedQueryException e) {
            fail();
        }
    }
}
