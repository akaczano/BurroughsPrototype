package com.viasat.burroughs;

import org.junit.After;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class BasicQueryTest extends BurroughsTest {

    @Test
    public void testBasicQuery1() throws InterruptedException {
        checkConditions();
        burroughs.setDbTable("test_basic_1");
        try {
            Map<String, Double> sums = new HashMap<>();
            sums.put("WEST", 90496.51);
            sums.put("SOUTH", 73864.21);
            sums.put("EAST", 110530.85);
            sums.put("CENTRAL", 82263.01);

            Map<String, Double> avgs = new HashMap<>();
            avgs.put("WEST", 1.3070521243977223);
            avgs.put("SOUTH", 1.26657074340527577938);
            avgs.put("EAST", 1.29207488602192259191);
            avgs.put("CENTRAL", 1.3111341901423688);

            burroughs.processQuery("select storer, sum(spend) as total, avg(units) as unit_avg from test_data group by 1;");
            Thread.sleep(5000);
            assertNotNull(burroughs.queryStatus().getTableStatus());
            assertNotNull(burroughs.queryStatus().getConnectorStatus());
            assertTrue(burroughs.queryStatus().getConnectorStatus().isConnectorRunning());
            assertTrue(burroughs.queryStatus().getConnectorStatus().getErrors().size() < 1);
            assertTrue(burroughs.queryStatus().getTableStatus().hasStatus());

            waitForQuery();

            Statement stmt = db.createStatement();
            ResultSet results = stmt.executeQuery("select * from test_basic_1;");
            while (results.next()) {
                String region = results.getString("storer");
                double sum = results.getDouble("total");
                double avg = results.getDouble("unit_avg");
                assertTrue(String.format("Wrong sum: expected %f, but was %f", sums.get(region), sum),
                        Math.abs(sums.get(region) - sum) < 0.001);
                assertTrue(String.format("Wrong average: expected %f, but was %f", avgs.get(region), avg),
                        Math.abs(avgs.get(region) - avg) < 0.001);
            }
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
            int count = 24;
            burroughs.setDbTable("test_having");
            burroughs.processQuery("select basketnum, sum(units) from test_data group by basketnum having sum(units) > 20");
            waitForQuery();
            Statement stmt = db.createStatement();
            ResultSet results = stmt.executeQuery("select count(*) from test_having;");
            assertTrue(results.next());
            int actual = results.getInt(1);
            assertEquals(String.format("Expected %d records, but found %d.", count, actual), actual, count);
        }
        catch(Exception e) {
            e.printStackTrace();
            fail();
        }
    }


    @After
    public void dispose() {
        burroughs.stop(false);
    }
}
