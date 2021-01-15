package com.viasat.burroughs;

/**
 * This interface provides a way of exposing database fields in the Burroughs
 * class to other components that need database access without exposing all of the
 * Burroughs functionality.
 */
public interface DBProvider {
    String getDbHost();
    String getDbUser();
    String getDbPassword();
    String getDatabase();
    String getDbTable();
    String getConnectorDb();
}
