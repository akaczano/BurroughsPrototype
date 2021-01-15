package com.viasat.burroughs.service;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Service class containing methods for reading topic/partition metadata from Kafka
 * directly, without going through KsqlDB.
 */
public class KafkaService {

    private final String kafkaHost; // Kafka hostname
    private AdminClient adminClient; // AdminClient has the ability to read certain config info
    private KafkaConsumer<?, ?> consumer; // We need to initialize a consumer to check offsets

    /*
        We cache the last requested consumer group, topic, and partition that was requested.
        This means that while the first .status command takes a while, subsequent command are dramatically
        faster
     */
    private String groupCache = "";
    private String topicCache = "";
    private int partitionCache = -1;

    /**
     * Initializes a new KafkaService
     * @param kafkaHost The hostname and port of the broker
     */
    public KafkaService(String kafkaHost) {
        this.kafkaHost = kafkaHost;
    }

    /**
     * Get the current offsets for a particular consumer group in all of the partitions
     * it is reading from
     * @param consumerGroup The group to lookup
     * @return A symbol table mapping partitions to current offsets
     */
    public Map<TopicPartition, Long> getCurrentOffset(String consumerGroup) {
        Map<TopicPartition, Long> results = new HashMap<>();
        if (!consumerGroup.equals(groupCache)) {
            // If this is a new consumer group, we have to initialize a new AdminClient
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaHost);
            properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
            properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5001);
            adminClient = KafkaAdminClient.create(properties);
            groupCache = consumerGroup;
        }
        try {
            // Use the existing admin client to read the data
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

    /**
     * Gets the max offset for a partition (the total number of records)
     * @param consumerGroup The consumer group to use
     * @param tp The partition to lookup
     * @return The max offset
     */
    public Long getLogMaxOffset(String consumerGroup, TopicPartition tp) {
        if (!consumerGroup.equals(groupCache) ||
                !(topicCache.equals(tp.topic()) && partitionCache == tp.partition())) {
            // If this is a new group or topic partition we have to initialize a new consumer
            // This is an expensive operation
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
        // As far as I know this is the only way to find the max offset
        consumer.seekToEnd(Collections.singleton(tp));

        return consumer.position(tp);
    }
}
