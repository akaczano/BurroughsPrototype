package com.viasat.burroughs.client;

import com.viasat.burroughs.Burroughs;
import com.viasat.burroughs.execution.ExecutionException;
import com.viasat.burroughs.logging.ConsoleLogger;
import com.viasat.burroughs.logging.Logger;
import com.viasat.burroughs.producer.ProducerEntry;
import com.viasat.burroughs.producer.ProducerInterface;
import com.viasat.burroughs.producer.ProducerStatus;
import com.viasat.burroughs.service.model.burroughs.BurroughsConnection;
import com.viasat.burroughs.service.model.burroughs.TableStatus;
import com.viasat.burroughs.service.model.list.Topic;
import com.viasat.burroughs.service.model.burroughs.QueryStatus;
import com.viasat.burroughs.validation.TopicNotFoundException;
import com.viasat.burroughs.validation.UnsupportedQueryException;
import org.apache.avro.Schema;
import org.apache.calcite.sql.parser.SqlParseException;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.io.*;
import java.util.*;

/**
 * Class that handles all CLI commands and prints responses
 */
public class BurroughsCLI implements Completer {

    private final Burroughs burroughs;

    /**
     * Symbol table that maps commands to the method that handles
     * them. Only the first word of the command, which always starts
     * with '.', is used as the key.
     */
    private final Map<String, CommandHandler> handlers;

    /**
     * Constructor. Initialize handlers table
     *
     * @param burroughs
     */
    public BurroughsCLI(Burroughs burroughs) {
        this.burroughs = burroughs;
        this.handlers = new HashMap<>();
        this.handlers.put(".stop", this::handleStop);
        this.handlers.put(".table", this::handleTable);
        this.handlers.put(".topics", this::handleTopics);
        this.handlers.put(".topic", this::handleTopic);
        this.handlers.put(".help", this::handleHelp);
        this.handlers.put(".connect", this::handleConnect);
        this.handlers.put(".connection", this::handleConnection);
        this.handlers.put(".status", this::handleStatus);
        this.handlers.put(".quit", this::handleQuit);

        this.handlers.put(".delete", this::handleDeletion);
        this.handlers.put(".file", this::handleFile);

        this.handlers.put(".debug", this::handleDebug);
        this.handlers.put(".file", this::handleFilein);
        this.handlers.put(".producers", this::handleProducers);
        this.handlers.put(".producer", this::handleProducer);
        this.handlers.put(".cleanup", this::handleCleanUp);
    }

    /**
     * Method that receives all raw text from the JLine terminal.
     *
     * @param command The command to handle.
     */
    public void handleCommand(String command) {
        if (command == null) return;
        try {
            command = command.trim(); // We always trim off whitespace
            BurroughsConnection conn = burroughs.connection();

            // Only the .connect and .connection commands can run without connection
            // to KsqlDB and PostgreSQL established
            if ((!conn.isKsqlConnected() || !conn.isDbConnected()) && !command.equals(".connect") &&
                    !command.equals(".connection") && !command.equals(".quit")) {
                System.out.println("Connection not established");
                System.out.println("Use .connect to re-connect");
                System.out.println("Use .connection to view connection info");
                return;
            }
            if (command.startsWith(".")) {
                // Lookup command in handlers and execute the correct one
                String commandWord = command.split("\\s+")[0];
                if (this.handlers.containsKey(commandWord)) {
                    this.handlers.get(commandWord).handle(command);
                } else {
                    System.out.println("Unknown command: " + commandWord);
                }
            } else {
                // If it doesn't start with a period, we assume it's a SQL query.
                try {
                    burroughs.processQuery(command);
                } catch (SqlParseException | TopicNotFoundException | UnsupportedQueryException e) {
                    System.out.printf("%sValidation error: %s%s\n",
                            ConsoleLogger.ANSI_RED, e.getMessage(), ConsoleLogger.ANSI_RESET);
                }
            }
        } catch (ExecutionException e) {
            // Display error
            System.out.println(e.getMessage());
            System.out.println("Use .debug for more info.");
        }
    }

    /**
     * Prints the schema for the specified topic
     *
     * @param command The command string
     */
    private void handleTopic(String command) {
        String[] words = command.split("\\s+");
        if (words.length != 2) {
            System.out.println("Usage: .topic <topic>");
        } else {
            String topicName = words[1];
            Schema schema = burroughs.topic(topicName);

            System.out.println("Field Name: Type");
            for (Schema.Field f : schema.getFields()) {
                System.out.printf("%s: %s\n", f.name(), f.schema().getName());
            }
        }
    }

    /**
     * Prints a list of available topics
     *
     * @param command Command string starting with .topics
     */
    private void handleTopics(String command) {
        Topic[] list = burroughs.topics();
        for (Topic t : list) {
            System.out.println(t);
        }
    }

    /**
     * Prints out DebugLevels String
     *
     * @param command Command string starting with .debug
     */
    private void handleDebug(String command) {
        String[] words = command.split("\\s+");
        ConsoleLogger logger = (ConsoleLogger) Logger.getLogger();
        if (words.length == 1) {
            System.out.println("Usage: .debug <value>, where value = 1 or 2 ");
            return;
        } else if (Integer.parseInt(words[1]) == 1) {
            System.out.println("Preliminary Traceback:" + '\n');
            logger.displayDebug(Logger.LEVEL_1);
        } else if (Integer.parseInt(words[1]) == 2) {
            System.out.println("Preliminary Traceback:" + '\n');
            logger.displayDebug(Logger.LEVEL_2);
        } else {
            System.out.println("Error.  Invalid value. Please use value = 1 or 2.");
        }

    }


    /**
     * Takes in files
     *
     * @param command Command string starting with .topics
     */
    private void handleFilein(String command) {
        System.out.println("This is the current directory" + System.getProperty("user.dir"));
    }

    /**
     * Called when the .connection command is executed.
     * Prints connection status.
     *
     * @param command Not used.
     */
    private void handleConnection(String command) {
        BurroughsConnection conn = burroughs.connection();
        System.out.printf("ksqlDB Hostname: %s, Status: %s%s%s\n",
                conn.getKsqlHost(),
                conn.isKsqlConnected() ? ConsoleLogger.ANSI_GREEN : ConsoleLogger.ANSI_RED,
                conn.isKsqlConnected() ? "Connected" : "Disconnected",
                ConsoleLogger.ANSI_RESET);
        System.out.printf("PostgreSQL Hostname: %s, Status: %s%s%s\n",
                conn.getdBHost(),
                conn.isDbConnected() ? ConsoleLogger.ANSI_GREEN : ConsoleLogger.ANSI_RED,
                conn.isDbConnected() ? "Connected" : "Disconnected",
                ConsoleLogger.ANSI_RESET);
    }

    /**
     * Handles the .stop command
     *
     * @param command The command string beginning with .stop
     */
    private void handleStop(String command) {
        boolean keepTable = Arrays.stream(command.split("\\s+"))
                .anyMatch(w -> w.equalsIgnoreCase("keep-table"));
        burroughs.stop(keepTable);
    }

    /**
     * Corresponds to the .table command. Sets the current output table
     *
     * @param command Command string
     */
    private void handleTable(String command) {
        String[] words = command.split("\\s+");

        if (words.length == 1) {
            if (burroughs.getDbTable() == null) {
                System.out.println("No table selected yet.");
            } else {
                System.out.println(burroughs.getDbTable());
            }
        } else if (words.length > 2) {
            System.out.println("Usage: .table <tablename>");
        } else {
            burroughs.setDbTable(words[1]);
            System.out.println("Set output table to " + words[1]);
        }
    }

    /**
     * Called when the .connect command is run. Simply calls the init method again
     * in an attempt to establish connection.
     *
     * @param command Not used.
     */
    private void handleConnect(String command) {
        System.out.println("Connecting...");
        burroughs.init();
    }

    /**
     * Prints the status of the active query
     *
     * @param command Command string beginning with .status
     */
    private void handleStatus(String command) {
        QueryStatus status = burroughs.queryStatus();
        System.out.println("Status:");
        if (status == null) {
            System.out.println("There is no active query. Enter some SQL to execute one.");
            return;
        }
        System.out.printf("Active Query ID: %s\n", status.getQueryId());
        TableStatus tableStatus = status.getTableStatus();
        if (tableStatus == null) {
            System.out.println("Table not created");
            return;
        }
        if (!tableStatus.hasStatus()) {
            System.out.println("Status not available");
            return;
        }
        System.out.printf("Process rate: %f messages/s\n", tableStatus.getProcessRate());
        System.out.printf("Total messages processed: %d\n", tableStatus.getTotalMessages());
        for (int i = 0; i < tableStatus.getQueryOffsets().size(); i++) {
            long current = tableStatus.getQueryOffsets().get(i);
            long max = tableStatus.getQueryMaxOffsets().get(i);
            System.out.printf("Query %d: %d%% (%d/%d)\n",
                    i + 1, (int) ((((double) current) / max) * 100), current, max);
        }
        double totalProgress = (((double) tableStatus.getTotalProgress()) /
                tableStatus.getTotalWork());
        System.out.printf("Total Progress: %d%% (%d/%d)\n",
                (int) (totalProgress * 100),
                tableStatus.getTotalProgress(), tableStatus.getTotalWork());
        System.out.printf("Total run time: %.1f seconds\n", tableStatus.getTotalRuntime() / 1000.0);
        System.out.printf("Estimated time remaining: %.1f seconds\n",
                ((tableStatus.getTotalRuntime() / (totalProgress)) - tableStatus.getTotalRuntime()) / 1000.0);
        if (status.getConnectorStatus() != null) {
            if (!status.getConnectorStatus().isConnectorRunning()) {
                System.out.println(ConsoleLogger.ANSI_YELLOW + "Connector not running" + ConsoleLogger.ANSI_RESET);
            } else {
                for (String error : status.getConnectorStatus().getErrors()) {
                    System.out.println(ConsoleLogger.ANSI_RED + "Connector Error:" + ConsoleLogger.ANSI_RESET);
                    System.out.println(error);
                }
            }
        }
    }

    /**
     * Exits Burroughs
     *
     * @param command Command string beginning with .quit
     */
    private void handleQuit(String command) {
        burroughs.dispose();
        System.out.println("Goodbye!");
        System.exit(0);
    }

    /**
     * Reads in a sql commands from a file, line by line
     *
     * @param command Command string beginning with .file
     */
    private void handleFile(String command) {
        String[] words = command.split("\\s+");
        if (words.length != 3) {
            System.out.println("Usage: .file <filename> <delimiter>");
        } else {
            String filename = words[1];
            String delimiter = words[2];
            File file = new File("/commands/" + filename);
            BufferedReader br;
            ArrayList<String> commands = new ArrayList<>();
            try {
                br = new BufferedReader(new FileReader(file));
                String line = br.readLine();
                while (line != null) {
                    commands.add(line);
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            StringBuilder sb = new StringBuilder();
            for (String c : commands) {
                sb.append(c).append(' ');
            }
            String[] commandList = sb.toString().split(delimiter);
            for (int i = 0; i < commandList.length - 1; i++) {
                this.handleCommand(commandList[i]);
            }

        }
    }

    private void handleProducers(String command) {
        List<ProducerEntry> producers = burroughs.producerInterface().getProducers();
        System.out.println("Producers:");
        for (ProducerEntry p : producers) {
            System.out.println(p.getName());
        }
    }

    /**
     * Handles all producer commands
     *
     * @param command The command, starting with .producer
     */
    private void handleProducer(String command) {
        ProducerInterface producerInterface = burroughs.producerInterface();
        String[] words = command.split("\\s+");
        if (words.length < 3) {
            System.out.println("Usage: .producer <producer name> <command> [arguments]");
            return;
        }
        if (!producerInterface.hasProducer(words[1])) {
            System.out.printf("Could not find producer %s\n", words[1]);
            return;
        }

        String name = words[1];
        String op = words[2];
        if (op.equalsIgnoreCase("status")) {
            ProducerStatus producerStatus = producerInterface.getProducerStatus(name);
            if (producerStatus.getStatus() == ProducerStatus.NOT_STARTED) {
                System.out.println("Producer not started");
                return;
            }
            System.out.printf("Producer status: %s\n", producerStatus.toString());
            if (producerStatus.getErrorMessage() != null) {
                System.out.printf("Error Message: %s\n", producerStatus.getErrorMessage());
            }
            System.out.printf("Records produced: %d\n", producerStatus.getRecordsProduced());
            System.out.printf("Records lost: %d\n", producerStatus.getRecordsLost());
        } else if (op.equalsIgnoreCase("pause")) {
            if (words.length > 3) {
                try {
                    int time = Integer.parseInt(words[3]);
                    producerInterface.pauseProducer(name, time);
                } catch (NumberFormatException e) {
                    System.out.printf("Invalid delay %s\n", words[3]);
                }
            } else {
                producerInterface.pauseProducer(name);
            }
        } else if (op.equalsIgnoreCase("resume")) {
            producerInterface.resumeProducer(name);
        } else if (op.equalsIgnoreCase("kill")) {
            producerInterface.terminateProducer(name);
        } else if (op.equalsIgnoreCase("start")) {
            if (words.length > 3) {
                try {
                    int limit = Integer.parseInt(words[3]);
                    producerInterface.startProducer(name, limit);
                } catch (NumberFormatException e) {
                    System.out.printf("Invalid limit %s\n", words[3]);
                }
            } else {
                producerInterface.startProducer(name, -1);
            }
        } else if (op.equalsIgnoreCase("set-delay")) {
            if (words.length < 4) {
                System.out.println("Usage: .producer <producer> set-delay delay (ms)");
            } else {
                try {
                    int delay = Integer.parseInt(words[3]);
                    System.out.printf("Changed delay from %d to %d\n",
                            producerInterface.getProducerDelay(name), delay);
                    producerInterface.setProducerDelay(name, delay);
                } catch (NumberFormatException e) {
                    System.out.printf("Invalid delay: %s\n", words[3]);
                }
            }
        } else {
            System.out.printf("Unknown operation: %s\n", op);
        }
    }

    private void handleDeletion(String command) {
        String[] words = command.split("\\s+");
        if (words.length < 2) {
            System.out.println("Usage: .delete <topic_name>");
            return;
        }

        for (Topic t : burroughs.topics()) {
            if (t.getName().equals(words[1])) {
                System.out.println("Dropping it ");
                burroughs.dropTopic(t.getName());
                return;
            }
        }
        System.out.println("This topic does not seem to exist");
    }

    public void handleCleanUp(String command) {
        if (!burroughs.cleanUp()) {
            System.out.println("You must stop the current query before running cleanup.");
        }
    }

    /**
     * Prints the instructions
     *
     * @param command Not used.
     */
    private void handleHelp(String command) {
        System.out.println("Available Commands");
        System.out.println(".help");
        System.out.println("\tPrints a list of commands.");
        System.out.println(".table");
        System.out.println("\tPrints the currently selected output table.");
        System.out.println(".table <tablename>");
        System.out.println("\tSets the output table to tablename.");
        System.out.println(".topics");
        System.out.println("\tPrints a list of available topics.");
        System.out.println(".topic <topic>:");
        System.out.println("\tPrints the schema for the specified topic.");
        System.out.println(".status");
        System.out.println("\tPrints the status of the currently executing query.");
        System.out.println(".stop [keep-table]");
        System.out.println("\tHalts query execution, removes all associated ksqlDB objects and " +
                "\n\tdrops output table unless keep-table is specified.");
        System.out.println(".connection");
        System.out.println("\tDisplays connection information/status.");
        System.out.println(".connect");
        System.out.println("\tAttempts to reconnect to ksqlDB and PostgreSQL");
        System.out.println(".producers");
        System.out.println("\tDisplays a list of producers");
        System.out.println(".delete <topic>");
        System.out.println("\tDeletes the specified topic from the Kafka stream");
        System.out.println(".producer <producer> <operation> [arguments]");
        System.out.println("\tExecutes the given command for the specified producer.");
        System.out.println("\tAvailable operations");
        System.out.println("\tstart [limit]: starts the producer");
        System.out.println("\tstatus: prints the producer's status");
        System.out.println("\tpause [delay (ms)]: pauses the producer indefinitely or for a length of time");
        System.out.println("\tresume: resumes producer operation");
        System.out.println("\tkill: stops producer operation");
        System.out.println("\tset-delay delay (ms): sets the artificial delay between messages");

        System.out.println(".debug <debug value>");
        System.out.println("\tdebug value: \n\t1 = shows the kSQL query(ies) executed;  2 = shows more in-depth traceback of query transformation");

        System.out.println(".file <file name> <delimiter>");
        System.out.println("\tReads and executes commands and/or a query from the specified file.");
        System.out.println(".quit");
        System.out.println("\tExits burroughs. Ctrl+D works too.");
        System.out.println("Any other input will be treated like a SQL query.");
    }

    /**
     * Computes auto-complete candidates
     *
     * @param lineReader The associated line reader
     * @param parsedLine The line of text, already parsed
     * @param list       The list of candidates for completion
     */
    @Override
    public void complete(LineReader lineReader, ParsedLine parsedLine, List<Candidate> list) {
        for (String command : this.handlers.keySet()) {
            if (command.startsWith(parsedLine.line())) {
                list.add(new Candidate(command));
            }
        }

        if (parsedLine.words().get(0).equalsIgnoreCase(".producer")) {
            for (String producer : burroughs.producerInterface().getList()) {
                if (parsedLine.words().size() == 1 ||
                        (parsedLine.words().size() == 2 && producer.startsWith(parsedLine.words().get(1)))) {
                    list.add(new Candidate(producer));
                } else if (parsedLine.words().size() >= 2 &&
                        burroughs.producerInterface().getList().contains(parsedLine.words().get(1))) {
                    for (String op : ProducerInterface.COMMAND_LIST) {
                        list.add(new Candidate(op));
                    }
                }
            }
        }
    }

}
