package com.viasat.pipeline;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Base class for any producer using this schema/topic
 * Encapsulates all of the Kafka interactions behind simple interface.
 */
public abstract class ProducerBase extends Thread implements Callback {

    private final KafkaProducer<String, Object> producer;
    private  Schema schema;

    protected ProducerBase (String broker) {
        Properties properties = new Properties();
        // localhost:29092
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);
        properties.put("schema.registry.url", Main.SCHEMA_REGISTRY);
        producer = new KafkaProducer<>(properties);

        Schema.Parser parser = new Schema.Parser();
        try {
            InputStream stream = getClass().getClassLoader().getResourceAsStream("transaction.avsc");
            schema = parser.parse(stream);
        }
        catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to load schema from file");
        }
    }


    /**
     * Sends a message to the transaction topic
     * @param fields An ordered list of fields to be encoded using the Avro schema
     */
    protected void send(Object[] fields) {
        GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < fields.length; i++) {
            record.put(schema.getFields().get(i).name(), fields[i]);
        }
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<String, Object>(Main.TOPIC,
                record.get("StoreR").toString(), record);
        producer.send(producerRecord, this);
    }

    /**
     * Error handling:
     * Prints out the stack trace of any exception thrown during message send.
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        }
    }
}
