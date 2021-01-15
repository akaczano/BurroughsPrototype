package com.viasat.burroughs.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class KafkaService {

    private final String kafkaHost;
    private AdminClient adminClient;
    private KafkaConsumer<?, ?> consumer;

    private String groupCache = "";
    private String topicCache = "";
    private int partitionCache = -1;

    public KafkaService(String kafkaHost) {
        this.kafkaHost = kafkaHost;
    }

    public Map<TopicPartition, Long> getCurrentOffset(String consumerGroup) {
        Map<TopicPartition, Long> results = new HashMap<>();
        if (!consumerGroup.equals(groupCache)) {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaHost);
            properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
            properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5001);
            adminClient = KafkaAdminClient.create(properties);
            groupCache = consumerGroup;
        }
        try {
            Map<TopicPartition, OffsetAndMetadata> map = adminClient
                    .listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata().get();
            for (TopicPartition tp : map.keySet()) {
                results.put(tp, map.get(tp).offset());
            }
            return results;
        } catch(InterruptedException | ExecutionException e) {
            return null;
        }
    }

    public Long getLogMaxOffset(String consumerGroup, TopicPartition tp) {
        if (!consumerGroup.equals(groupCache) ||
                !(topicCache.equals(tp.topic()) && partitionCache == tp.partition())) {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaHost);
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
           consumer = new KafkaConsumer<>(properties);
           groupCache = consumerGroup;
           topicCache = tp.topic();
           partitionCache = tp.partition();
        }
        consumer.assign(Collections.singleton(tp));
        consumer.seekToEnd(Collections.singleton(tp));

        return consumer.position(tp);
    }
}
