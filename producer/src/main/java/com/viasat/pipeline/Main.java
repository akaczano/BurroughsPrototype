package com.viasat.pipeline;

public class Main {

    // Kafka
    public static final String BROKER = "broker:29092";
    public static final String SCHEMA_REGISTRY = "http://schema-registry:8081";
    public static final String TOPIC = "transactions";

    // Data IO
    public static final String OUTFILE = "/datafiles/output_data.csv";
    public static final String INFILE = "/datafiles/transactions.csv";
    public static final String DB_CONNECTION = "jdbc:postgresql://postgre:5432/postgres";

    // Parameters
    public static final String CONSUME_CSV = "consume-csv";
    public static final String CONSUME_DB = "consume-db";
    public static final String PRODUCE_CSV = "produce-csv";
    public static final String PRODUCE_DB = "produce-db";

    public static void main(String[] args) {
        if (args[0].equalsIgnoreCase(CONSUME_CSV)) {
            CSVConsumer consumer = new CSVConsumer(BROKER, OUTFILE);
            consumer.start();
        }
        else if (args[0].equalsIgnoreCase(CONSUME_DB)){
            DBConsumer consumer = new DBConsumer(BROKER);
            consumer.start();
        }
        else if (args[0].equalsIgnoreCase(PRODUCE_CSV)) {
            int limit = -1;
            if (args.length > 1) {
                try {
                    limit = Integer.parseInt(args[1]);
                }
                catch (NumberFormatException e) {
                    System.out.println("Invalid limit entered: " + args[1]);
                }
            }
            CSVProducer producer = new CSVProducer(BROKER, INFILE, limit);
            producer.start();
        }
        else if (args[0].equalsIgnoreCase(PRODUCE_DB)) {
            int limit = -1;
            if (args.length > 1) {
                try {
                    limit = Integer.parseInt(args[1]);

                } catch (NumberFormatException e) {
                    System.out.println("Invalid count entered: " + args[1]);
                }
            }

            DBProducer producer = new DBProducer(BROKER, limit);
            producer.start();
        }
        else {
            System.out.println("Invalid argument");
        }
    }

}
