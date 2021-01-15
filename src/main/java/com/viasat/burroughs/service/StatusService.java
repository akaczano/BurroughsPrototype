package com.viasat.burroughs.service;

import com.google.gson.Gson;
import com.viasat.burroughs.service.model.HealthStatus;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Contains methods for determining the status of a KsqlDB server.
 */
public class StatusService {

    private final String hostname;

    public StatusService(String hostname) {
        this.hostname = hostname;
    }

    /**
     * Performs a healthcheck and returns the status.
     * @return A HealthStatus object containing the health information
     */
    public HealthStatus checkConnection() {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(hostname + "/healthcheck"))
                .build();
        try {
            HttpResponse<String> response = client.send(request,
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return new Gson().fromJson(response.body(), HealthStatus.class);
            }

        } catch (IOException | InterruptedException e){
            return null;
        }
        return null;
    }

}
