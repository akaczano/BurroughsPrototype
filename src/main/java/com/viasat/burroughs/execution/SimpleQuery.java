package com.viasat.burroughs.execution;

import com.viasat.burroughs.service.KafkaService;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.model.list.Format;


public class SimpleQuery extends QueryBase {

    private final String query;

    private String stream = null;
    private String table = null;
    private String connector = null;

    public SimpleQuery(StatementService service, KafkaService kafkaService,
                       QueryProperties props, String query) {
        super(service, kafkaService, props);
        this.query = query;
    }

    @Override
    public void execute() {
        System.out.printf("Creating stream %s...", properties.getStreamName());
        if (!streamExists(properties.getStreamName())) {
            createStream(properties.getStreamName(), properties.getTopicName(), Format.AVRO);
            System.out.print("Done\n");
        }
        else {
            System.out.println("\nStream already exists");
        }
        System.out.print("Creating table from stream...");
        createTable(properties.getId(), query);
        System.out.print("Done\n");
        System.out.print("Linking to database...");
        createConnector(properties.getId());
        System.out.print("Done\n");
        startTime = System.currentTimeMillis();
    }

    @Override
    public void destroy() {
        if (table != null) {
            System.out.print("Dropping table " + table + "...");
            dropTable(table);
            System.out.print("Done\n");
        }
        if (stream != null) {
            System.out.print("Dropping stream " + stream + "...");
            dropStream(stream);
            System.out.print("Done\n");
        }
        if (connector != null) {
            System.out.print("Dropping connector " + connector + "...");
            dropConnector(connector);
            System.out.print("Done\n");
        }
        System.out.print("Dropping output table...");
        dropOutput();
        System.out.print("Done\n");
    }

    @Override
    public void printStatus() {
        if (table != null) {
            super.printStatisticsForTable(this.table);
        }
        else {
            System.out.println("Table not created");
        }
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
