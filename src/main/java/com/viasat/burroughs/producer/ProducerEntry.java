package com.viasat.burroughs.producer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.viasat.burroughs.DBProvider;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerEntry {

    // Producer config
    private String name;
    private volatile int delay = 0;
    private int maxRecords = Integer.MAX_VALUE;
    private Schema schema;
    private String topic;
    private String keyField;
    private IDataSource dataSource;

    private Producer producer;

    /**
     * Initializes a producer object and starts the producer tread
     * @param kafka The kafka hostname/port
     * @param schemaRegistry The schema registry url
     * @param maxRecords The maximum number of records to produce
     */
    public void buildAndStart(String kafka, String schemaRegistry, int maxRecords) {
        if (producer != null && (producer.getStatus().equals("Running") || producer.getStatus().equals("Paused"))) {
            System.out.println("producer is already running");
            return;
        }
        if (maxRecords >= 0) {
            this.maxRecords = maxRecords;
        }
        producer = new Producer(kafka, schemaRegistry, this);
        producer.start();
    }

    /**
     * Pauses producer execution until resumed
     */
    public void pause() {
        if (producer == null) {
            System.out.println("Producer not started");
            return;
        }
        producer.pauseProducer();
    }


    /**
     * Pauses the producer for the specified duration of time or until resumed
     * @param time Time span in milliseconds
     */
    public void pause(int time) {
        if (producer == null) {
            System.out.println("Producer not started");
            return;
        }
        producer.pauseProducer(System.currentTimeMillis() + time);
    }

    /**
     * Resumes producer operation or does nothing if not paused
     */
    public void resume() {
        if (producer == null) {
            System.out.println("Producer not started");
            return;
        }
        producer.resumeProducer();
    }

    /**
     * Terminates the producer
     */
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

    public ProducerStatus getStatus() {
        if (producer == null) {
            return new ProducerStatus(ProducerStatus.NOT_STARTED, 0, 0);
        }
        ProducerStatus status = new ProducerStatus(ProducerStatus.valueOf(producer.getStatus()),
                producer.getRecordsProduced(), producer.getFailedRecords());
        if (producer.getErrorMessage().length() > 0) {
            status.setErrorMessage(producer.getErrorMessage());
        }
        return status;
    }


    public void setDelay(int delay) { this.delay = delay; }

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

    /**
     * Takes a JSON file containing an array of producer configurations
     * and parses it into a list of ProducerEntry objects
     * @param file The file to parse
     * @param defaultDB The database to use as the default
     * @return A list of producer configurations
     */
    public static List<ProducerEntry> parse(Path file, DBProvider defaultDB) {
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
                else if (ds.get("type").getAsString().equalsIgnoreCase("database")) {
                    String hostName = defaultDB.getDbHost();
                    String database = defaultDB.getDatabase();
                    String username = defaultDB.getDbUser();
                    String password = defaultDB.getDbPassword();

                    if (source.has("hostname")) {
                        hostName = source.get("hostname").getAsString();
                    }
                    if (source.has("database")) {
                        database = source.get("database").getAsString();
                    }
                    if (source.has("username")) {
                        username = source.get("username").getAsString();
                    }
                    if (source.has("password")) {
                        password = source.get("password").getAsString();
                    }

                    String conString = String.format("jdbc:postgresql://%s/%s",
                            hostName, database);
                    Properties props = new Properties();
                    props.put("user", username);
                    props.put("password", password);
                    try {
                        Connection connect = DriverManager.getConnection(conString, props);
                        producer.dataSource = new DBSource(connect, producer.schema, source.get("table").getAsString());
                    } catch(SQLException e) {
                        System.out.printf("Failed to load producer %s\n", producer.name);
                        e.printStackTrace();
                        continue;
                    }
                }
                producers.add(producer);
            }
        } catch(IOException e) {
            return null;
        }
        return producers;
    }


    /**
     * Validates the producer json to ensure required properties are there
     * @param o The Producer json object
     * @return The validation error message or null if successful
     */
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
        else if (type.equalsIgnoreCase("database")) {
            JsonObject source = dataSource.get("source").getAsJsonObject();
            if (!source.has("table"))
                return "Database source must specify table";
        }
        else {
            return "Data source type must be one of file or database";
        }
        return null;
    }

}
