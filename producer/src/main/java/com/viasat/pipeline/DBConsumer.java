package com.viasat.pipeline;


import org.apache.avro.generic.GenericRecord;

import java.sql.*;
import java.util.Properties;

public class DBConsumer extends ConsumerBase {

    private Connection conn;
    private PreparedStatement insertStatement = null;
    private int recordsInBatch = 0;
    private long lastBatch = 0;

    public static final int BATCH_SIZE = 30;



    public DBConsumer(String broker) {
        super(broker);
        Properties props = new Properties();
        props.put("user", "postgres");
        props.put("password", "password");

        try {
            conn = DriverManager.getConnection(Main.DB_CONNECTION, props);

            String query = "INSERT INTO Output(basketnum, date, productnum, spend, units, region) VALUES (?,?,?,?,?,?);";
            insertStatement = conn.prepareStatement(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void onPause() {
		System.out.println("Paused...");
        try {
            if (insertStatement != null && recordsInBatch > 0) {
                insertStatement.executeBatch()  ;
                recordsInBatch = 0;
            }
        }
        catch(SQLException e) {
            e.printStackTrace();
        }
    }

    public void onMessage(long offset, String key, GenericRecord value) {
        if (conn != null) {
            try {

                insertStatement.setInt(1, (int)value.get("BasketNum"));
                insertStatement.setString(2, value.get("Date").toString());
                insertStatement.setString(3, value.get("ProductNum").toString());
                insertStatement.setDouble(4, (double)value.get("Spend"));
                insertStatement.setInt(5, (int)value.get("Units"));
                insertStatement.setString(6, value.get("StoreR").toString());
                insertStatement.addBatch();
                System.out.printf("%s,%s,%s,%s,%s,%s\n",
                        value.get("BasketNum").toString(),
                        value.get("Date").toString(),
                        value.get("ProductNum").toString(),
                        value.get("Spend").toString(),
                        value.get("Units").toString(),
                        value.get("StoreR").toString());
                recordsInBatch++;
                if (recordsInBatch == BATCH_SIZE || (System.currentTimeMillis() - lastBatch > 1000 && recordsInBatch > 0)) {
                    insertStatement.executeBatch();
                    recordsInBatch = 0;
                    lastBatch = System.currentTimeMillis();
                }

               } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


}
