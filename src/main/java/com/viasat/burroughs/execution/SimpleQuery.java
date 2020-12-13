package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.list.Format;

import java.util.Properties;

public class SimpleQuery extends QueryBase {

    private final String query;
    private final String topicName;
    private final String streamName;

    private String stream = null;
    private String table = null;
    private String connector = null;

    public SimpleQuery(StatementService service, Properties props, String query,
                       String topicName, String streamName) {
        super(service, props);
        this.query = query;
        this.topicName = topicName;
        this.streamName = streamName;
    }

    @Override
    public void execute(String id) {
        if (!streamExists(streamName)) {
            createStream(streamName, topicName, Format.AVRO);
        }
        createTable(id, query);
        createConnector(id);
    }

    @Override
    public void destroy() {
        if (table != null) {
            dropTable(table);
        }
        if (stream != null) {
            dropStream(stream);
        }
        if (connector != null) {
            dropConnector(connector);
        }
        dropOutput();
    }

    @Override
    protected String createStream(String streamName, String topic, Format format) {
        return (stream = super.createStream(streamName, topic, format));
    }

    @Override
    protected String createTable(String id, String query) {
        return (table = super.createTable(id, query));
    }

    @Override
    protected String createConnector(String id) {
        return (connector = super.createConnector(id));
    }
}
