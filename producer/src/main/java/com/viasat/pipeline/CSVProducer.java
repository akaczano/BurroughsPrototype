package com.viasat.pipeline;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class CSVProducer extends ProducerBase {

    /**
     * Input file
     */
    private final File file;

    private int limit;

    /**
     * Initialize KafkaProducer (super class constructor)
     *
     * @param broker Connection string for Kafka broker
     * @param filename The name of CSV file to read from
     */
    public CSVProducer(String broker, String filename, int limit) {
        super(broker);
        // Creat file object
        this.file = new File(filename);
        this.limit = limit;
    }

    /**
     * Where the action happens. Records are read from a CSV file and produced to a
     * Kafka topic.
     */
    @Override
    public void run() {
        boolean first = true;
        int counter = 0;
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (first) {
                    // We skip the header line
                    first = false;
                    continue;
                }
                if (counter == limit) {
                    break;
                }

                String[] data = line.split(",");

                // Trim off whitespace
                for (int i = 0; i < data.length; i++) {
                    data[i] = data[i].trim();
                }
                try {
                    super.send(new Object[]{
                            Integer.parseInt(data[0]), // basket num
                            data[1], // date
                            data[2], // product num
                            Double.parseDouble(data[3]), // spend
                            Integer.parseInt(data[4]), // units
                            data[5] // store region
                    });
                    counter++;
                } catch (NumberFormatException e) {
                    continue;
                }

            }
            System.out.printf("Produced %d records to topic %s\n", counter, Main.TOPIC);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
