package com.viasat.burroughs.service.model;

import com.google.gson.annotations.SerializedName;

public abstract class StatementResponse {

    @SerializedName("@type")
    private String type;

    private String statementText;

    private Warning[] warnings;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStatementText() {
        return statementText;
    }

    public void setStatementText(String statementText) {
        this.statementText = statementText;
    }

    public Warning[] getWarnings() {
        return warnings;
    }

    public void setWarnings(Warning[] warnings) {
        this.warnings = warnings;
    }
}
