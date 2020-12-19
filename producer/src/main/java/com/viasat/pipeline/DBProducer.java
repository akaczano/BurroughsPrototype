package com.viasat.pipeline;

import java.sql.*;
import java.util.Properties;

public class DBProducer extends ProducerBase {

    /**
     * Database connection object
     */
    private Connection conn;

    private int limit;

    /**
     * Initialize producer:
     * Super class constructor initializes the KafkaProducer object
     * This constructor initializes the database connection
     * @param broker
     */
    public DBProducer(String broker, int limit) {
        super(broker);
        Properties props = new Properties();
        props.put("user", "postgres");
        props.put("password", "password");
        try {
            conn = DriverManager
                    .getConnection(Main.DB_CONNECTION, props);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.limit = limit;
    }

    @Override
    public void run() {
        if (conn != null) {
            Statement stmt = null;
            String query = "SELECT * FROM transactions";
            if (limit != -1) {
                query += String.format(" LIMIT(%d)", limit);
            }
            int count = 0;
            try {
                stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next()) {
                    super.send(new Object[]{
                            rs.getInt("basket_num"),
                            rs.getDate("purchase_date").toString().trim(),
                            rs.getString("product_num").trim(),
                            rs.getDouble("spend"),
                            rs.getInt("units"),
                            rs.getString("region").trim()
                    });
                    count++;
                }
                System.out.printf("Produced %d records to topic %s\n", count, Main.TOPIC);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
