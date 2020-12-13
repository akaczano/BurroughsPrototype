package com.viasat.burroughs.service;


import com.google.gson.Gson;
import com.viasat.burroughs.service.model.HealthStatus;
import com.viasat.burroughs.service.model.StatementError;
import com.viasat.burroughs.service.model.StatementResponse;
import com.viasat.burroughs.service.model.body.StatementHolder;
import com.viasat.burroughs.service.model.body.StreamProperties;
import com.viasat.burroughs.service.model.command.CommandResponse;
import com.viasat.burroughs.service.model.description.DescribeResponse;
import com.viasat.burroughs.service.model.list.ListResponse;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class StatementService {

    public static final String path = "/ksql";

    private final String hostname;

    public StatementService(String ksqlHostname) {
        hostname = ksqlHostname + path;
    }


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

    public StatementResponse executeStatement(String statement) {
        return executeStatement(statement, new StreamProperties());
    }

    public StatementResponse executeStatement(String statement, StreamProperties properties) {
        try {
            statement = statement.trim();
            String up = statement.toUpperCase();
            Class<? extends StatementResponse> format;
            if (up.startsWith("CREATE") || up.startsWith("TERMINATE") || up.startsWith("DROP")) {
                format = CommandResponse.class;
            }
            else if (up.startsWith("SHOW") || up.startsWith("LIST")) {
                format = ListResponse.class;
            }
            else if (up.startsWith("DESCRIBE")) {
                format = DescribeResponse.class;
            }
            else {
                throw new IllegalArgumentException("Invalid Command");
            }
            Object result = execute(statement, properties, format);
            if (result instanceof StatementResponse) {
                return (StatementResponse) result;
            } else {
                return (StatementError) result;
            }
        } catch(IOException | InterruptedException e) {
            return null;
        }
    }
}
