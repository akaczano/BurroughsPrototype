package com.viasat.burroughs.producer;

import com.google.gson.JsonSyntaxException;
import com.viasat.burroughs.DBProvider;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * This class manages the list of loaded producers and
 * processes any commands run against it.
 */
public class ProducerInterface {

    private final HashMap<String, ProducerEntry> producers;
    private final String kafkaHost;
    private final String schemaRegistry;

    public static final String[] COMMAND_LIST = {
            "status", "pause", "resume", "kill", "start", "set-delay"
    };

    /**
     * Initializes the interface and parses the list of producers
     * to be found in the default producer file location.
     * @param kafkaHost Kafka hostname
     * @param schemaRegistry Schema registry URL
     * @param defaultDB The default database to use
     */
    public ProducerInterface(String kafkaHost, String schemaRegistry, DBProvider defaultDB) {
        this.kafkaHost = kafkaHost;
        this.schemaRegistry = schemaRegistry;
        producers = new HashMap<>();
        System.out.println("Loading producers...");
        File producerFile = new File("/producer/producers.json");
        if (!producerFile.exists()) {
            System.out.println("No producers found");
        }
        else {
            try {
                List<ProducerEntry> entries = ProducerEntry.parse(producerFile.toPath(), defaultDB);
                if (entries == null) {
                    System.out.println("Unable to load producers.");
                } else {
                    System.out.printf("%d producers loaded\n", entries.size());
                    for (ProducerEntry entry : entries) {
                        producers.put(entry.getName(), entry);
                    }
                }
            } catch(JsonSyntaxException e) {
                System.out.printf("Invalid producers.json file: %s\n", e.getMessage());
            }
        }

    }

    public boolean hasProducer(String producer) {
        return producers.containsKey(producer);
    }

    public ProducerStatus getProducerStatus(String name) {
        return producers.get(name).getStatus();
    }

    public void pauseProducer(String name) {
        producers.get(name).pause();
    }

    public void pauseProducer(String name, int time) {
        producers.get(name).pause(time);
    }

    public void resumeProducer(String name) {
        producers.get(name).resume();
    }

    public void terminateProducer(String name) {
        producers.get(name).terminate();
    }

    public void startProducer(String name, int limit) {
        producers.get(name).buildAndStart(kafkaHost, schemaRegistry, limit);
    }

    public int getProducerDelay(String name) {
        return producers.get(name).getDelay();
    }

    public void setProducerDelay(String name, int delay) {
        producers.get(name).setDelay(delay);
    }

    /**
     * Prints a list of producers. Handles the .producers command
     */
    public List<ProducerEntry> getProducers() {
        return new ArrayList<>(this.producers.values());
    }

    public Set<String> getList() {
        return this.producers.keySet();
    }

    /**
     * Called upon application shutdown. Makes sure all of the producer threads
     * are terminated.
     */
    public void stopProducers() {
        for (ProducerEntry p : producers.values()) {
            p.terminate();
        }
    }

}
