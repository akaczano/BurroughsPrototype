package com.viasat.burroughs.producer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ProducerEntry {

    private String name;
    private int delay = 0;
    private int maxRecords = Integer.MAX_VALUE;
    private Schema schema;
    private String topic;
    private String keyField;
    private IDataSource dataSource;

    private String status = null;

    private Producer producer;

    public void buildAndStart(String kafka, String schemaRegistry, int maxRecords) {
        if (maxRecords >= 0) {
            this.maxRecords = maxRecords;
        }
        producer = new Producer(kafka, schemaRegistry, this);
        producer.start();
    }

    public void pause() {
        if (producer == null) {
            System.out.println("Producer not started");
            return;
        }
        producer.pauseProducer();
    }

    public void pause(int time) {
        if (producer == null) {
            System.out.println("Producer not started");
            return;
        }
        producer.pauseProducer(System.currentTimeMillis() + time);
    }

    public void resume() {
        if (producer == null) {
            System.out.println("Producer not started");
            return;
        }
        producer.resumeProducer();
    }

    public void terminate() {
        if (producer == null) {
            return;
        }
        if (!producer.getStatus().equalsIgnoreCase("Stopped")) {
            System.out.printf("Stopping producer %s...\n", this.name);
        }
        producer.stopProducer();
        producer = null;
    }

    public void printStatus() {
        System.out.printf("Producer %s status\n", this.name);
        if (producer == null) {
            System.out.println("Producer not started");
            return;
        }
        System.out.printf("Producer status: %s\n", producer.getStatus());
        System.out.printf("Records produced: %d\n", producer.getRecordsProduced());
        System.out.printf("Records lost: %d\n", producer.getFailedRecords());
    }

    public String getName() {
        return this.name;
    }

    public int getDelay() {
        return this.delay;
    }

    public int getMaxRecords() {
        return this.maxRecords;
    }

    public String getKeyField() {
        return this.keyField;
    }

    public IDataSource getDataSource() {
        return this.dataSource;
    }
    public Schema getSchema() {
        return this.schema;
    }

    public String getTopic() {
        return this.topic;
    }

    public static List<ProducerEntry> parse(Path file) {
        List<ProducerEntry> producers = new ArrayList<>();
        try {
            String text = Files.readString(file);
            JsonArray array = (new Gson()).fromJson(text, JsonArray.class);
            for (JsonElement s : array) {
                ProducerEntry producer = new ProducerEntry();
                JsonObject o = s.getAsJsonObject();

                // JSON validation
                String message = validate(o);
                if (message != null) {
                    System.out.println("Invalid producer: " + message);
                    continue;
                }

                // Basic configuration
                producer.name = o.get("name").getAsString();
                producer.topic = o.get("topic").getAsString();
                if (o.has("delay")) {
                    producer.delay = o.get("delay").getAsInt();
                }

                // Schema
                File schemaFile = new File("/producer/" + o.get("schema").getAsString());
                if (!schemaFile.exists()) {
                    System.out.printf("Unable to find schema file %s\n", o.get("schema").getAsString());
                    continue;
                }
                Schema.Parser parser = new Schema.Parser();
                producer.schema = parser.parse(Files.readString(schemaFile.toPath()));
                if (o.has("key_field")) {
                    producer.keyField = o.get("key_field").getAsString();
                }
                // Data source
                JsonObject ds = o.get("data_source").getAsJsonObject();
                JsonObject source = ds.getAsJsonObject("source");
                if (ds.get("type").getAsString().equalsIgnoreCase("file")) {
                    FileSource fs = new FileSource(
                            new File("/producer/" + source.get("location").getAsString()),
                            producer.schema
                    );
                    if (source.has("delimiter")) {
                        fs.setDelimiter(source.get("delimiter").getAsString());
                    }
                    if (source.has("header")) {
                        fs.setHasHeader(source.get("header").getAsBoolean());
                    }
                    producer.dataSource = fs;
                }
                producers.add(producer);
            }
        } catch(IOException e) {
            return null;
        }
        return producers;
    }



    private static String validate(JsonObject o) {
        if (!o.has("name"))
            return "Producer must have name property";
        if (!o.has("topic"))
            return "Producer must have topic property";
        if (!o.has("schema"))
            return "Producer must specify schema";
        if (!o.has("data_source"))
            return "Producer must have data_source";
        JsonObject dataSource = o.get("data_source").getAsJsonObject();
        if (dataSource == null)
            return "Producer data source cannot be null";
        if (!dataSource.has("type"))
            return "Data source must specify type";
        String type = dataSource.get("type").getAsString();
        if (!dataSource.has("source"))
            return "Data source must have source object";
        if (type.equalsIgnoreCase("file")) {
            JsonObject source = dataSource.get("source").getAsJsonObject();
            if (!source.has("location"))
                return "File source must specify file location";
        }
        else if (type.equalsIgnoreCase("db")) {
            //TODO
        }
        else {
            return "Data source type must be one of file or db";
        }
        return null;
    }

}
