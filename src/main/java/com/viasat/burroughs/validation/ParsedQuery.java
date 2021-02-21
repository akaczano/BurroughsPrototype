package com.viasat.burroughs.validation;

import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

public class ParsedQuery {
    private final SqlSelect query;
    private final SqlNodeList withList;
    private final int limit;

    public ParsedQuery(SqlSelect query, SqlNodeList withList, int limit) {
        this.query = query;
        this.withList = withList;
        this.limit = limit;
    }

    public SqlSelect getQuery() {
        return query;
    }

    public SqlNodeList getWithList() {
        return withList;
    }

    public int getLimit() {
        return limit;
    }
}
