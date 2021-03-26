package com.viasat.burroughs.service;

import com.google.gson.Gson;
import com.viasat.burroughs.service.model.description.Field;
import com.viasat.burroughs.service.model.schema.Subject;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class SchemaService {

    private String hostname;

    public SchemaService(String hostname) {
        this.hostname = hostname;
    }

    public Subject getSchema(String topicName) {

        int[] versions = getVersions(topicName);
        if (versions == null) return null;

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(String.format(
                "%s/subjects/%s-value/versions/%d", hostname, topicName, versions[versions.length - 1] )))
                .build();
        try {
            HttpResponse<String> response = client.send(request,
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return new Gson().fromJson(response.body(), Subject.class);
            }

        } catch (IOException | InterruptedException e){
            return null;
        }
        return null;
    }

    private int[] getVersions(String topicName) {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create(String.format(
                "%s/subjects/%s-value/versions", hostname, topicName))).build();
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return (int[]) new Gson().fromJson(response.body(), int.class.arrayType());
            }
            return null;
        }
        catch(IOException | InterruptedException e) {
            return null;
        }
    }

}
