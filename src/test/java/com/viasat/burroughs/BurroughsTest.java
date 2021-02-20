package com.viasat.burroughs;

import com.viasat.burroughs.execution.ExecutionException;
import com.viasat.burroughs.service.model.burroughs.BurroughsConnection;
import com.viasat.burroughs.service.model.list.Topic;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
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
        } catch (ExecutionException e) {
            System.out.println("test_data needs to be created");
        }
        burroughs.producerInterface().startProducer("test_producer", -1);

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

    protected void waitForQuery() {
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

    }

}
