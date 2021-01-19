package com.viasat.burroughs.producer;

import com.google.gson.JsonSyntaxException;
import com.viasat.burroughs.DBProvider;

import java.io.File;
import java.util.HashMap;
import java.util.List;

/**
 * This class manages the list of loaded producers and
 * processes any commands run against it.
 */
public class ProducerInterface {

    private final HashMap<String, ProducerEntry> producers;
    private final String kafkaHost;
    private final String schemaRegistry;

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

    /**
     * Handles all producer commands
     * @param command The command, starting with .producer
     */
    public void handleCommand(String command) {
        String[] words = command.split("\\s+");
        if (words.length < 3) {
            System.out.println("Usage: .producer <producer name> <command> [arguments]");
            return;
        }
        if (!producers.containsKey(words[1])) {
            System.out.printf("Could not find producer %s\n", words[1]);
            return;
        }

        ProducerEntry entry = producers.get(words[1]);
        String op = words[2];
        if (op.equalsIgnoreCase("status")) {
            entry.printStatus();
        }
        else if (op.equalsIgnoreCase("pause")) {
            if (words.length > 3) {
                try {
                    int time = Integer.parseInt(words[3]);
                    entry.pause(time);
                } catch(NumberFormatException e) {
                    System.out.printf("Invalid delay %s\n", words[3]);
                }
            }
            else {
                entry.pause();
            }
        }
        else if (op.equalsIgnoreCase("resume")) {
            entry.resume();
        }
        else if (op.equalsIgnoreCase("kill")) {
            entry.terminate();
        }
        else if (op.equalsIgnoreCase("start")) {
            if (words.length > 3) {
                try {
                    int limit = Integer.parseInt(words[3]);
                    entry.buildAndStart(kafkaHost, schemaRegistry, limit);
                } catch(NumberFormatException e) {
                    System.out.printf("Invalid limit %s\n", words[3]);
                }
            }
            else {
                entry.buildAndStart(kafkaHost, schemaRegistry, -1);
            }
        }
        else if (op.equalsIgnoreCase("set-delay")) {
            if (words.length < 4) {
                System.out.println("Usage: .producer <producer> set-delay delay (ms)");
            }
            else {
                try {
                    int delay = Integer.parseInt(words[3]);
                    System.out.printf("Changed delay from %d to %d\n",
                            entry.getDelay(), delay);
                    entry.setDelay(delay);
                } catch(NumberFormatException e) {
                    System.out.printf("Invalid delay: %s\n", words[3]);
                }
            }
        }
        else {
            System.out.printf("Unknown operation: %s\n", op);
        }
    }

    /**
     * Prints a list of producers. Handles the .producers command
     */
    public void printList() {
        System.out.println("Producers:");
        for (String name : this.producers.keySet()) {
            System.out.println(name);
        }
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
