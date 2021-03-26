package com.viasat.burroughs.service.model.schema;

import org.apache.avro.Schema;

public class Subject {
    private final String subject;
    private final int version;
    private final int id;
    private final String schema;

    public Subject(String subject, int version, int id, String schema) {
        this.subject = subject;
        this.version = version;
        this.id = id;
        this.schema = schema;
    }

    public String getSubject() {
        return subject;
    }

    public int getVersion() {
        return version;
    }

    public int getId() {
        return id;
    }


    public Schema getSchema() {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(this.schema);
    }

}
