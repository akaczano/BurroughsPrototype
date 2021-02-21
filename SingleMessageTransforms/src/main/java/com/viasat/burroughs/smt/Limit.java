package com.viasat.burroughs.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

public class Limit<R extends ConnectRecord<R>> implements Transformation<R> {

    private interface ConfigName {
        String LIMIT = "limit";
    }

    public static final String OVERVIEW_DOC = "Limits the number of records in the output table.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.LIMIT,
                    ConfigDef.Type.INT,
                    0,
                    ConfigDef.Importance.HIGH,
                    "Record limit");

    private int limit;
    private HashSet<String> keys;


    @Override
    public R apply(R record) {
        if (limit == 0) {
            return record;
        }
        else {
            String display = record.key().toString();

            if (record.key() instanceof byte[]) {
                display = new String((byte[])record.key());
            }
            if (keys.size() < limit) {
                keys.add(display);
                return record;
            }
            else {
                return keys.contains(display) ? record : null;
            }
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        limit = config.getInt(ConfigName.LIMIT);
        keys = new HashSet<>();
    }

}
