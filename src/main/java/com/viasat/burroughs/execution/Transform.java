package com.viasat.burroughs.execution;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Transform {

    private final String name;
    private final String type;
    private final Map<String, String> properties;

    public Transform(String name, String type) {
        this.name = name;
        this.type = type;
        this.properties = new HashMap<>();
    }

    public void addProperty(String name, String value) {
        properties.put(name, value);
    }

    public Map<String, String> properties() {
        return this.properties;
    }

    public String name() {
        return this.name;
    }

    public String type() {
        return this.type;
    }

    @Override
    public String toString() {
        StringBuilder config = new StringBuilder("");
        config.append(String.format("'transforms.%s.type' = '%s',", name, type));
        for (String prop : properties.keySet()) {
            config.append(String.format("'transforms.%s.%s' = '%s',", name, prop, properties.get(prop)));
        }
        return config.toString();
    }

    public static String header(List<Transform> transforms) {
        StringBuilder names = new StringBuilder("");
        for (Transform t : transforms) {
            if (names.length() > 0) names.append(",");
            names.append(t.name);
        }
        return String.format("'transforms' = '%s',", names);
    }

}
