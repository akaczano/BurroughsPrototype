package com.viasat.burroughs.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import javax.print.attribute.standard.PresentationDirection;
import java.util.Optional;
import java.util.Properties;

public class Producer extends Thread implements Callback {

    private final ProducerEntry config;
    private final int maxRecords;
    private final IDataSource source;
    private final String keyField;
    private final String topic;
    private final KafkaProducer<Object, Object> producer;

    private volatile int counter = 0;
    private volatile int failedRecords = 0;
    private volatile boolean stopped = false;
    private volatile boolean paused = false;
    private volatile long resumeTime = 0;
    private String status = "Not started";

    public Producer(String kafkaHost, String schemaRegistry, ProducerEntry config) {
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

    public String getStatus() {
        if (paused || resumeTime > System.currentTimeMillis()) {
            return "Paused";
        }
        return status;
    }

    public int getRecordsProduced() {
        return counter;
    }

    public int getFailedRecords() {
        return failedRecords;
    }

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
                        new ProducerRecord<Object, Object>(topic, key, record);
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
        } catch (Exception e) {
            e.printStackTrace();
            status = "Error";
        }
        if (status.equals("Running")) status = "Stopped";
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            int cache = failedRecords;
            failedRecords = cache + 1;
        }
    }

    public void stopProducer() {
        stopped = true;
    }

    public void pauseProducer() {
        paused = true;
    }

    public void pauseProducer(long resumeTime) {
        this.resumeTime = resumeTime;
    }

    public void resumeProducer() {
        this.resumeTime = 0;
        this.paused = false;
    }

}
