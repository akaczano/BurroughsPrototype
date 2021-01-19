package com.viasat.burroughs.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.util.Optional;
import java.util.Properties;

public class Producer extends Thread implements Callback {

    /**
     * Contains configuration for the producer
     */
    private final ProducerEntry config;

    /**
     * The maximum number of records to produce
     */
    private final int maxRecords;

    /**
     * The data source to pull from
     */
    private final IDataSource source;

    /**
     * The field to use as the record key
     */
    private final String keyField;

    /**
     * The topic to produce data to
     */
    private final String topic;

    /**
     * The producer object
     */
    private final KafkaProducer<Object, Object> producer;

    // Various properties that control live producer operation
    private volatile int counter = 0;
    private volatile int failedRecords = 0;
    private volatile boolean stopped = false;
    private volatile boolean paused = false;
    private volatile long resumeTime = 0;

    /**
     * Current status
     */
    private String status = "Not started";
    private String errorMessage = "";
    /**
     * Initializes a producer
     * @param kafkaHost Hostname and port of the Kafka broker
     * @param schemaRegistry URL of the AVRO schema registry
     * @param config Object containing producer configuration
     */
    public Producer(String kafkaHost, String schemaRegistry, ProducerEntry config) {
        // Load configuration
        this.maxRecords = config.getMaxRecords();
        this.source = config.getDataSource();
        this.keyField = config.getKeyField();
        this.topic = config.getTopic();
        this.config = config;

        Schema schema = config.getSchema();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put("schema.registry.url", schemaRegistry);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);

        // Determine the key serializer class based on the schema and key field
        if (keyField != null) {
            Optional<Field> potField = schema.getFields().stream()
                    .filter(f -> f.name().equalsIgnoreCase(keyField))
                    .findFirst();
            if (potField.isEmpty()) {
                throw new ProducerException(String.format("Field %s not found", keyField));
            }
            String type = potField.get().schema().getType().getName();
            String configName = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
            if (type.equalsIgnoreCase("int")) {
                props.put(configName, IntegerSerializer.class);
            } else if (type.equalsIgnoreCase("string")) {
                props.put(configName, StringSerializer.class);
            } else if (type.equalsIgnoreCase("long")) {
                props.put(configName, LongSerializer.class);
            } else if (type.equalsIgnoreCase("double")) {
                props.put(configName, DoubleSerializer.class);
            } else {
                props.put(configName, ByteArraySerializer.class);
            }
        }
        else {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        }

        producer = new KafkaProducer<>(props);
    }

    /**
     * Returns a simple string description of the producer status
     * @return The producer's status
     */
    public String getStatus() {
        if (paused || resumeTime > System.currentTimeMillis()) {
            return "Paused";
        }
        return status;
    }

    /**
     * Returns the last error encountered by the producer if applicable
     * @return The error message
     */
    public String getErrorMessage() {
        return this.errorMessage;
    }

    /**
     * Gets the total number of records produced
     * @return An integer representing the total number of records produced
     */
    public int getRecordsProduced() {
        return counter;
    }

    /**
     * Gets the total of numbers that failed to reach the broker
     * @return The number of failures
     */
    public int getFailedRecords() {
        return failedRecords;
    }

    /**
     * The main run method. Continuously reads records from the data source
     * and produces them to the kafka topic unless paused.
     */
    @Override
    public void run() {
        if (!source.checkAvailability()) {
            status = "Failed to read from source";
            return;
        }
        status = "Running";
        try {
            source.open();
            while (source.hasNextRecord() && !stopped) {
                while (paused && !stopped) Thread.yield();
                while (System.currentTimeMillis() < resumeTime && !stopped) Thread.yield();
                if (stopped) break;
                if (counter == maxRecords) break;
                GenericRecord record = source.nextRecord();
                Object key = keyField == null ? record : record.get(keyField);
                ProducerRecord<Object, Object> item =
                        new ProducerRecord<>(topic, key, record);
                producer.send(item, this);
                int cache = counter;
                counter = cache + 1;
                try {
                    Thread.sleep(config.getDelay());
                } catch (InterruptedException e) {
                    break;
                }
            }
            source.close();
        } catch (ProducerException e) {
            status = "Error";
            errorMessage = e.getMessage();
        }
        if (status.equals("Running")) status = "Stopped";
    }

    /**
     * Producer callback. Increments the number of failed messages
     * if there was an error
     * @param recordMetadata Record metadata
     * @param e Error
     */
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            int cache = failedRecords;
            failedRecords = cache + 1;
        }
    }

    /**
     * Terminates the producer and exits the thread
     */
    public void stopProducer() {
        stopped = true;
    }

    /**
     * Pauses the producer until resumed.
     */
    public void pauseProducer() {
        paused = true;
    }

    /**
     * Pauses the producer for the specified amount of time
     * @param resumeTime The pause duration in milliseconds
     */
    public void pauseProducer(long resumeTime) {
        this.resumeTime = resumeTime;
    }

    /**
     * Causes the producer to resume producing
     */
    public void resumeProducer() {
        this.resumeTime = 0;
        this.paused = false;
    }

}
