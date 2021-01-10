package com.viasat.burroughs;

import static org.junit.Assert.assertTrue;

import com.viasat.burroughs.producer.Producer;
import com.viasat.burroughs.producer.ProducerEntry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Scanner;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() throws IOException {
        List<ProducerEntry> producers = ProducerEntry.parse(Path.of("producers.json"));
        for (ProducerEntry entry : producers) {
            entry.buildAndStart("localhost:9092", "http://localhost:8081");
        }
       // while (producers.stream().anyMatch(p -> p.getStatus().equals("Running")));
    }
}
