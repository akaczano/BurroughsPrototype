package edu.purdue.datamine.burroughsserver;

import com.viasat.burroughs.DBProvider;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class ConnectionHolder {
    private Connection connection;
    private final DBProvider dbProvider;

    public ConnectionHolder(DBProvider db) {
        dbProvider = db;
    }

    public void init() {
        String connStr = String.format("jdbc:postgresql://%s/%s",
                dbProvider.getDbHost(), dbProvider.getDatabase());
        Properties props = new Properties();
        props.put("user", dbProvider.getDbUser());
        props.put("password", dbProvider.getDbPassword());
        try {
            connection = DriverManager.getConnection(connStr, props);
        } catch (SQLException e) {
            connection = null;
        }
    }

    public ArrayList<Object[]> getSnapshot(String query) throws SQLException {
        ArrayList<Object[]> data = new ArrayList<>();
        if (connection != null) {
            Statement stmt = connection.createStatement();
            ResultSet results = stmt.executeQuery(query);
            int count = results.getMetaData().getColumnCount();
            Object[] header = new Object[count];
            for (int i = 1; i <= count; ++i) {
                header[i - 1] = results.getMetaData().getColumnLabel(i);
            }
            data.add(header);
            while (!results.isLast()) {
                results.next();
                Object[] row = new Object[count];
                for (int i = 1; i <= count; i++) {
                    row[i - 1] = results.getObject(i);
                }
                data.add(row);
            }

        }
        return data;
    }
}
