package com.viasat.burroughs;

import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class AdvancedQueryTest extends BurroughsTest {

    /*
    @Test
    public void testGroupConcat() {

    }

    @Test
    public void testSimpleSubquery() {

    }

    @Test
    public void testSimpleCTE() {

    }*/

    @Test
    public void testMultipleCTE() {
        try {
            checkConditions();
            String table = "pairs_query";
            String query = "with transactions3 as (\n" +
                    "    select * from test_data\n" +
                    "), pairs as (\n" +
                    "    select\n" +
                    "        it1.productnum as source_item,\n" +
                    "        it2.productnum as target_item\n" +
                    "    from test_data it1\n" +
                    "    inner join transactions3 it2\n" +
                    "        on it1.basketnum = it2.basketnum\n" +
                    "        and it1.productnum < it2.productnum\n" +
                    "    where\n" +
                    "        it1.units > 0\n" +
                    "        and it2.units > 0\n" +
                    ")\n" +
                    "select\n" +
                    "    source_item,\n" +
                    "    target_item,\n" +
                    "count(1) as frequency\n" +
                    "from pairs\n" +
                    "group by 1,2\n" +
                    "having count(1) > 2";

            burroughs.setDbTable(table);
            burroughs.processQuery(query);
            waitForQuery();
            compareCount(query, table);
            Map<String, Class> fieldMap = new HashMap<>();
            fieldMap.put("frequency", Integer.class);
            compareFields(query, table, fieldMap, "source_item,target_item");
        } catch(Exception e) {
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
