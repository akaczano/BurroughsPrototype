package com.viasat.burroughs;

import com.google.common.hash.HashingInputStream;
import org.junit.After;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class BasicQueryTest extends BurroughsTest {

    @Test
    public void testBasicQuery1() throws InterruptedException {
        checkConditions();
        burroughs.setDbTable("test_basic_1");
        try {
            String query = "select storer, sum(spend) as total, avg(units) as unit_avg from test_data group by 1;";
            burroughs.processQuery(query);

            waitForQuery();
            compareCount(query, "test_basic_1");
            Map<String, Class> fieldMap = new HashMap<>();
            fieldMap.put("total", Double.class);
            fieldMap.put("unit_avg", Double.class);
            compareFields(query, "test_basic_1", fieldMap,"storer");

            burroughs.stop(false);
        }
        catch (Exception e) {
            e.printStackTrace();
            burroughs.stop(false);
            fail();
        }
    }

    @Test
    public void testHaving() throws InterruptedException {
        checkConditions();
        try {
            String table = "test_having";
            String query = "select basketnum, sum(units) as total from test_data group by basketnum having sum(units) > 20";
            burroughs.setDbTable(table);
            burroughs.processQuery(query);
            waitForQuery();
            compareCount(query, table);
            Map<String, Class> fields = new HashMap<>();
            fields.put("total", Double.class);
            compareFields(query, table, fields, "basketnum");
        }
        catch(Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testWhere() throws InterruptedException {
        checkConditions();
        try {
            String query = "select basketnum, sum(units) from test_data where storer = 'CENTRAL' group by 1;";
            burroughs.setDbTable("test_where");
            burroughs.processQuery(query);
            waitForQuery();
            compareCount(query, "test_where");
        }
        catch(Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testSimpleJoin() throws InterruptedException {
        checkConditions();
        String query = "select custid, sum(spend) as TotalSpend, avg(units) as AverageUnits" +
                " from test_data t inner join test_customers c " +
                "on t.basketnum = c.basketnum group by 1";
        try {
            burroughs.setDbTable("test_simple_join");
            burroughs.processQuery(query);
            waitForQuery();
            compareCount(query, "test_simple_join");
            Map<String, Class> fieldMap = new HashMap<>();
            fieldMap.put("TotalSpend", Double.class);
            fieldMap.put("AverageUnits", Double.class);
            compareFields(query, "test_simple_join", fieldMap, "custid");
        } catch(Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testCombo() throws InterruptedException{
        checkConditions();
        String table = "test_combo1";
        String query = "select custid, sum(units) as TotalUnits from " +
                "test_data t inner join test_customers c " +
                "on t.basketnum = c.basketnum " +
                "where custid > 550620" +
                "group by custid";
        try {
            burroughs.setDbTable(table);
            burroughs.processQuery(query);
            waitForQuery();
            compareCount(query, table);
            Map<String, Class> fieldMap = new HashMap<>();
            fieldMap.put("TotalUnits", Double.class);
            compareFields(query, table, fieldMap, "custid");
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testLimit() {
        try {
            checkConditions();
            String query = "select storer, sum(spend) as TotalSpend from test_data group by 1 limit 2;";
            String table = "limit_test";
            burroughs.setDbTable(table);
            burroughs.processQuery(query);
            waitForQuery();
            ResultSet actualCount = db.createStatement().executeQuery("select count(*) from limit_test;");
            assertTrue(actualCount.next());
            assertEquals(2, actualCount.getInt(1));
            ResultSet actual = db.createStatement().executeQuery("select * from limit_test;");
            while (actual.next()) {
                String lookup = String.format("select sum(spend) from test_data where storer like '%%%s%%';",
                        actual.getString("storer"));
                ResultSet results = db.createStatement().executeQuery(lookup);
                assertTrue(results.next());
                double expectedVal = results.getDouble(1);
                double actualVal = actual.getDouble("TotalSpend");
                assertTrue(String.format("expected %f but was %f", expectedVal, actualVal),
                        Math.abs(expectedVal - actualVal) < 0.001);
            }

        } catch(Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGroupByDifferentTypes() {
        try {
            checkConditions();
            String table = "test_multi_group_type1";
            String query = "select storer, basketnum, avg(units) as AvgUnits from test_data group by 1,2";
            burroughs.setDbTable(table);
            burroughs.processQuery(query);
            waitForQuery();
            compareCount(query, table);
            Map<String, Class> fields = new HashMap<>();
            fields.put("basketnum", Integer.class);
            compareFields(query, table, fields, "storer,basketnum");
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGroupByDifferentTypes2() {
        try {
            checkConditions();
            String table = "test_multi_group_type2";
            String query = "select t.storer, c.basketnum, avg(units) as AvgUnits " +
                    "from test_data t inner join test_customers c on t.basketnum = c.basketnum " +
                    "group by 1,2";
            burroughs.setDbTable(table);
            burroughs.processQuery(query);
            waitForQuery();
            compareCount(query, table);
            Map<String, Class> fields = new HashMap<>();
            fields.put("basketnum", Integer.class);
            compareFields(query, table, fields, "storer,basketnum");
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGroupByDifferentTypes3() {
        try {
            checkConditions();
            String table = "test_multi_group_type3";
            String query = "select t.storer, custid, avg(units) as AvgUnits " +
                    "from test_data t inner join test_customers c on t.basketnum = c.basketnum " +
                    "group by 1,2";
            burroughs.setDbTable(table);
            burroughs.processQuery(query);
            waitForQuery();
            compareCount(query, table);
            Map<String, Class> fields = new HashMap<>();
            fields.put("custid", Integer.class);
            compareFields(query, table, fields, "storer,custid");
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @After
    public void dispose() {
        burroughs.stop(false);
        burroughs.dropTopic("test_data");
        burroughs.dropTopic("test_customers");
    }
}
