package com.viasat.burroughs.producer;

import com.google.gson.JsonSyntaxException;

import java.io.File;
import java.util.HashMap;
import java.util.List;

public class ProducerInterface {

    private HashMap<String, ProducerEntry> producers;
    private String kafkaHost;
    private String schemaRegistry;

    public ProducerInterface(String kafkaHost, String schemaRegistry) {
        this.kafkaHost = kafkaHost;
        this.schemaRegistry = schemaRegistry;

        System.out.println("Loading producers...");
        File producerFile = new File("/producer/producers.json");
        if (!producerFile.exists()) {
            System.out.println("No producers found");
        }
        else {
            producers = new HashMap<>();
            try {
                List<ProducerEntry> entries = ProducerEntry.parse(producerFile.toPath());
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

    public void handleCommand(String command) {
        String[] words = command.split("\\s+");
        if (words.length < 3) {
            System.out.println("Usage: .producer <producer name> <command> [arguments]");
            return;
        }
        if (!producers.containsKey(words[1])) {
            System.out.printf("Could not find producer %s\n", words[1]);
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

    public void printList() {
        System.out.println("Producers:");
        for (String name : this.producers.keySet()) {
            System.out.println(name);
        }
    }

    public void stopProducers() {
        for (ProducerEntry p : producers.values()) {
            p.terminate();
        }
    }

}
