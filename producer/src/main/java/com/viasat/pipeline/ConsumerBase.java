package com.viasat.pipeline;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public abstract class ConsumerBase extends Thread{

    private final KafkaConsumer<String, GenericRecord> consumer;
    private long lastMessage;

    public ConsumerBase(String broker) {
        // Initialize consumer
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group5");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put("schema.registry.url", Main.SCHEMA_REGISTRY);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(Main.TOPIC));
    }

    protected abstract void onMessage(long offset, String key, GenericRecord value);

    protected void onPause() {

    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    onMessage(record.offset(), record.key(), record.value());
                    lastMessage = System.currentTimeMillis();
                }
                if (System.currentTimeMillis() - lastMessage > 3000) {
                    onPause();
                    lastMessage = System.currentTimeMillis();
                }
            }
        }
        finally {
            consumer.close();
        }
    }
}
