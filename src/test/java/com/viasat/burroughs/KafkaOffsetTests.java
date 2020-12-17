package com.viasat.burroughs;

import com.viasat.burroughs.service.KafkaService;
import kafka.Kafka;
import kafka.admin.ConsumerGroupCommand;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaOffsetTests {

    @Test
    public void testOffsetLookup() throws ExecutionException, InterruptedException {
        KafkaService service = new KafkaService("localhost:9092");
        Map<TopicPartition, Long> current = service
                .getCurrentOffset("_confluent-ksql-default_query_CTAS_SUMS_0");
        for (TopicPartition tp : current.keySet()) {
            Long max = service.getLogMaxOffset("_confluent-ksql-default_query_CTAS_SUMS_0",
                            tp);
            Long now = current.get(tp);
            System.out.printf("%d/%d\n", now, max);
        }
        System.out.println("done");
    }
}
