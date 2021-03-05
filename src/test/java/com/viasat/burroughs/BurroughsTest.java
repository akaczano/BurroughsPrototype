package com.viasat.burroughs;

import com.viasat.burroughs.execution.ExecutionException;
import com.viasat.burroughs.service.model.burroughs.BurroughsConnection;
import com.viasat.burroughs.service.model.list.Topic;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public abstract class BurroughsTest {

    protected Burroughs burroughs;
    protected Connection db;


    public BurroughsTest() {
        Logger.setLogger(new ConsoleLogger());
        burroughs = new Burroughs();
        App.loadConfiguration(burroughs);
        burroughs.init();
        try {
            burroughs.dropTopic("test_data");
            burroughs.dropTopic("test_customers");
        } catch (ExecutionException e) {
            System.out.println("Topics not present");
        }
        burroughs.producerInterface().startProducer("test_producer", -1);
        burroughs.producerInterface().startProducer("test_customers", -1);

        String dbHost = burroughs.getDbHost();
        String database = burroughs.getDatabase();
        String dbUser = burroughs.getDbUser();
        String dbPassword = burroughs.getDbPassword();

        String conString = String.format("jdbc:postgresql://%s/%s", dbHost, database);
        Properties props = new Properties();
        props.put("user", dbUser);
        props.put("password", dbPassword);

        try {
            db = DriverManager.getConnection(conString, props);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected void checkConditions() throws InterruptedException {
        Thread.sleep(3000);
        BurroughsConnection conn = burroughs.connection();
        Assert.assertTrue(conn.isKsqlConnected());
        Assert.assertTrue(conn.isDbConnected());
        Assert.assertNotNull(db);
        Topic[] topics = burroughs.topics();
        Assert.assertTrue("Expected topic test_data to be present", Arrays.stream(topics).anyMatch(t ->
                t.getName().equalsIgnoreCase("test_data")));
    }

    protected void waitForQuery() throws InterruptedException {
        while (true) {
            if (burroughs.queryStatus().getTableStatus().hasStatus()) {
                long totalProgress = burroughs.queryStatus().getTableStatus().getTotalProgress();
                long totalWork = burroughs.queryStatus().getTableStatus().getTotalWork();
                if (totalWork > 0 && totalProgress == totalWork) {
                    break;
                }
            }
            Thread.yield();
        }
        BurroughsCLI cli = new BurroughsCLI(burroughs);
        cli.handleCommand(".status");
        Thread.sleep(3000);
    }

    protected void compareCount(String query, String table) throws SQLException {
        String countQuery = String.format("with query as (%s) select count(*) from query;",
                query.replace(";", ""));
        ResultSet expected = db.createStatement().executeQuery(countQuery);
        ResultSet actual = db.createStatement().executeQuery(String.format(
                "select count(*) from %s;", table));
        Assert.assertTrue(expected.next());
        Assert.assertTrue(actual.next());
        Assert.assertEquals(String.format("Expected %d records in table %s, but found %d.",
            expected.getInt(1), table, actual.getInt(1)),
                expected.getInt(1), actual.getInt(1));
    }

    protected void compareFields(String query, String table, Map<String, Class> fields, String key)
        throws SQLException {
        ResultSet expected = db.createStatement().executeQuery(String.format("%s order by %s;",
                query.replace(";", ""), key));

        StringBuilder selectList = new StringBuilder("select ");
        for (String field : fields.keySet()) {
            if (!selectList.toString().endsWith(" ")) {
                selectList.append(", ");
            }
            selectList.append(String.format("\"%s\"", field.toUpperCase()));
        }
        String orderby = "";
        for (String field : key.split(",")) {
            if (orderby.length() > 0) {
                orderby += ",";
            }
            orderby += "\"" + field.toUpperCase() + "\"";
        }
        selectList.append(String.format(" from %s order by %s;", table, orderby));
        ResultSet actual = db.createStatement().executeQuery(selectList.toString());
        while (expected.next()) {
            Assert.assertTrue(actual.next());
            for (String field : fields.keySet()) {
                if (fields.get(field) == Integer.class) {
                    int actualVal = actual.getInt(field);
                    int expectedVal = expected.getInt(field);
                    Assert.assertEquals(expectedVal, actualVal);
                }
                else if (fields.get(field) == String.class) {
                    String actualVal = actual.getString(field);
                    String expectedVal = expected.getString(field);
                    Assert.assertEquals(expectedVal, actualVal);
                }
                else if (fields.get(field) == Double.class) {
                    double actualVal = actual.getDouble(field);
                    double expectedVal = expected.getDouble(field);
                    Assert.assertTrue(Math.abs(expectedVal - actualVal) < 0.001);
                }
            }
        }
    }

}
