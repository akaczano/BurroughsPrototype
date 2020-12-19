package com.viasat.burroughs;


import com.viasat.burroughs.execution.QueryExecutor;
import com.viasat.burroughs.service.StatementService;
import com.viasat.burroughs.service.StatusService;
import com.viasat.burroughs.service.model.HealthStatus;
import com.viasat.burroughs.validation.QueryValidator;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
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

    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_RESET = "\u001B[0m";


    private static void loadConfiguration(Burroughs burroughs) {
        String ksqlHost = "http://localhost:8088";
        String dbHost = "localhost:5432";
        String database = "postgres";
        String dbUser = "postgres";
        String dbPassword = "";
        String kafkaHost = "localhost:9092";

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
        burroughs.setKsqlHost(ksqlHost);
        burroughs.setDbHost(dbHost);
        burroughs.setDatabase(database);
        burroughs.setDbUser(dbUser);
        burroughs.setDbPassword(dbPassword);
        burroughs.setKafkaHost(kafkaHost);
    }

    public static void main(String[] args) throws IOException {
        System.setErr(new PrintStream(System.err){
            public void println(String l) {
                if (!l.startsWith("SLF4J")) {
                    super.println(l);
                }
            }
        });
        System.out.println("Welcome to Burroughs!");
        Burroughs burroughs = new Burroughs();
        loadConfiguration(burroughs);
        burroughs.init();
        System.out.println("Not sure what to do now? Enter .help for a list of commands.");

        Terminal terminal = TerminalBuilder.terminal();
        LineReader lineReader = LineReaderBuilder.builder()
                .terminal(terminal)
                .build();

        String prompt = "sql>";
        while (true) {
            String line = null;
            try {
                line = lineReader.readLine(prompt);
            } catch (UserInterruptException e) {

            } catch (EndOfFileException e) {
                return;
            }
            burroughs.handleCommand(line);
        }
    }

}
