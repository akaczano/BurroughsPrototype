package com.viasat.burroughs.client;


import com.viasat.burroughs.Burroughs;
import com.viasat.burroughs.logging.ConsoleLogger;
import com.viasat.burroughs.logging.Logger;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

public class App {

    /**
     * All of the configuration for Burroughs, things like the hostnames
     * of ksqlD and Kafka, database connection info, are loaded from
     * environment variables. This method is called at startup to load
     * configuration from the environment variables when present and use
     * a reasonable default when not. See the README for the full list of environment
     * variables and their descriptions.
     * @param burroughs The Burroughs object to load configuration into
     */
    public static void loadConfiguration(Burroughs burroughs) {
        String ksqlHost = "http://localhost:8088";
        String dbHost = "localhost:5432";
        String connectorDB = "postgres:5432";
        String database = "burroughs";
        String dbUser = "postgres";
        String dbPassword = "password";
        String kafkaHost = "localhost:9092";
        String schemaRegistry = "http://localhost:8081";

        Map<String, String> env = System.getenv();
        if (env.containsKey("KSQL_HOST")) {
            ksqlHost = env.get("KSQL_HOST");
        }
        if (env.containsKey("DB_HOST")) {
            dbHost = env.get("DB_HOST");
        }
        if (env.containsKey("DB_USER")) {
            dbUser = env.get("DB_USER");
        }
        if (env.containsKey("DB_PASSWORD")) {
            dbPassword = env.get("DB_PASSWORD");
        }
        if (env.containsKey("DATABASE")) {
            database = env.get("DATABASE");
        }
        if (env.containsKey("KAFKA_HOST")) {
            kafkaHost = env.get("KAFKA_HOST");
        }
        if (env.containsKey("CONNECTOR_DB")) {
            connectorDB = env.get("CONNECTOR_DB");
        }
        if (env.containsKey("SCHEMA_REGISTRY")) {
            schemaRegistry = env.get("SCHEMA_REGISTRY");
        }
        if (env.containsKey("PRODUCER_PATH")) {
            burroughs.setProducerPath(env.get("PRODUCER_PATH"));
        }
        burroughs.setKsqlHost(ksqlHost);
        burroughs.setDbHost(dbHost);
        burroughs.setDatabase(database);
        burroughs.setDbUser(dbUser);
        burroughs.setDbPassword(dbPassword);
        burroughs.setKafkaHost(kafkaHost);
        burroughs.setConnectorDb(connectorDB);
        burroughs.setSchemaRegistry(schemaRegistry);
    }

    /**
     * Main method. This is where the JLine terminal is intialized and the REPL
     * is executed.
     * @param args Command line arguments (not used)
     * @throws IOException The JLine terminal could throw an IOException
     * which we can't really recover from
     */
    public static void main(String[] args) throws IOException {
        // This is used to filter out annoying warnings printed by SLF4J and log4j
        System.setErr(new PrintStream(System.err){
            public void println(String l) {
                if (!l.startsWith("SLF4J") && !l.startsWith("log4j")) {
                    super.println(l);
                }
            }
        });
        System.out.println("Welcome to Burroughs!");
        Logger.setLogger(new ConsoleLogger()); // Sets the default logger, which prints to stdout
        Burroughs burroughs = new Burroughs();
        loadConfiguration(burroughs);
        burroughs.init();
        System.out.println("Not sure what to do now? Enter .help for a list of commands.");

        BurroughsCLI cli = new BurroughsCLI(burroughs);

        // Create the JLine terminal
        Terminal terminal = TerminalBuilder.terminal();
        LineReader lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .completer(cli)
                .build();
        // The prompt that is shown when expecting user input
        String prompt = "sql>";
        while (true) {
            String line;
            try {
                line = lineReader.readLine(prompt);
            } catch (UserInterruptException e) {
                // This will execute when Ctrl+C is pressed
                continue;
            } catch (EndOfFileException e) {
                // This will execute when Ctrl+D is pressed
                burroughs.dispose(); // Stop producers
                return;
            }
            cli.handleCommand(line); // Pass the command to Burroughs
        }
    }

}
