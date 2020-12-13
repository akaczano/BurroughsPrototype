package com.viasat.burroughs.service.model.description;

import com.viasat.burroughs.service.model.StatementResponse;

import javax.xml.transform.Source;

public class DescribeResponse extends StatementResponse {
    private SourceDescription sourceDescription;

    public SourceDescription getSourceDescription() {
        return sourceDescription;
    }

    public void setSourceDescription(SourceDescription sourceDescription) {
        this.sourceDescription = sourceDescription;
    }
}
