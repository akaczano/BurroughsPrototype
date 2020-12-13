package com.viasat.burroughs;

public interface DBProvider {

    String getDbHost();
    String getDbUser();
    String getDbPassword();
    String getDatabase();
    String getDbTable();
}
