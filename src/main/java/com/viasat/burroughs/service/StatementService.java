package com.viasat.burroughs.service;


import com.google.gson.Gson;
import com.viasat.burroughs.execution.ExecutionException;
import com.viasat.burroughs.logging.Logger;
import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.body.StatementHolder;
import com.viasat.burroughs.service.model.body.StreamProperties;
import com.viasat.burroughs.service.model.command.CommandResponse;
import com.viasat.burroughs.service.model.description.ConnectorDescription;
import com.viasat.burroughs.service.model.description.DescribeResponse;
import com.viasat.burroughs.service.model.list.ListResponse;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;


//added


/**
 * Service class that provides utilities for executing Ksql statements
 */
public class StatementService {

    /**
     * The REST endpoint that all of the requests made by this class use
     */
    public static final String path = "/ksql";

    /**
     * URL for the ksqlDB REST proxy
     */
    private final String hostname;

    /**
     * Initializes a new StatementService object
     * @param ksqlHostname Full ksqlDB url
     */
    public StatementService(String ksqlHostname) {
        hostname = ksqlHostname + path;
    }


    /**
     * Executes a KSQL statement synchronously and returns the response
     * @param statement The statement to execute
     * @param props The StreamProperties to use
     * @param format A class that models the response
     * @return A deserialized object modeling the response or null if there was an error
     * @throws IOException Thrown if there is an error accessing the server
     * @throws InterruptedException Thrown if the thread is interrupted while the request is in progress
     */
    private Object execute(String statement, StreamProperties props,
                           Class<? extends StatementResponse> format)
            throws IOException, InterruptedException {
        StatementHolder body = new StatementHolder(statement, props);
        String raw = new Gson().toJson(body);
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(hostname))
                .method("POST", HttpRequest.BodyPublishers.ofString(raw))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200) {
            Object[] information = (Object[]) new Gson().fromJson(response.body(), format.arrayType());
            if (information.length > 0) {
                return information[0];
            }
        } else {
            return new Gson().fromJson(response.body(), StatementError.class);
        }
        return null;
    }

    /**
     * Execute the given statement and deserializes the response to the correct format
     * @param statement The statement to execute
     * @param properties The properties to use
     * @return The deserialized response or null if error
     */
    public StatementResponse executeStatement(String statement, StreamProperties properties) {
        try {
            statement = statement.trim();
            String up = statement.toUpperCase();
            Class<? extends StatementResponse> format;
            if (up.startsWith("CREATE") || up.startsWith("TERMINATE") || up.startsWith("DROP")) {
                Logger.getLogger().writeLine("executing: " + statement,
                        Logger.DEFAULT, Logger.LEVEL_2);
                format = CommandResponse.class;
            } else if (up.startsWith("SHOW") || up.startsWith("LIST")) {
                format = ListResponse.class;
            } else if (up.startsWith("DESCRIBE CONNECTOR")) {
                format = ConnectorDescription.class;
            } else if (up.startsWith("DESCRIBE")) {
                format = DescribeResponse.class;
            } else {
                throw new IllegalArgumentException("Invalid Command");
            }
            Object result = execute(statement, properties, format);
            if (result instanceof StatementResponse) {
                return (StatementResponse) result;
            } else {
                return (StatementError) result;
            }
        } catch (IOException | InterruptedException e) {
            return null;
        }
    }

    /**
     * Executes the given statement using the default StreamProperties
     * @param statement The KSQL statement to execute
     * @return A StatementResponse object or null if there was an error
     */
    public StatementResponse executeStatement(String statement) {
        return executeStatement(statement, new StreamProperties(true));
    }

    /**
     * This method contains the general error handling logic used for statement execution.
     * It will attempt to execute the statement and cast it to the specified type.
     * @param statement The KSQL statement to execute
     * @param message A description of what was being done to be used in the error message
     * @param <T> The type to cast the response to
     * @return An object modeling the response
     */
    public <T extends StatementResponse> T executeStatement(String statement, String message) {
        StatementResponse response = executeStatement(statement);
        if (response == null) {
            // If the response is null it means we couldn't get to the server
            throw new ExecutionException(String.format("Failed to %s due to connection error",
                    message));
        } else if (response instanceof StatementError) {
            throw new ExecutionException((StatementError) response);
        } else {
            return (T) response;
        }
    }

}
