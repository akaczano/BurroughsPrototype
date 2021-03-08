package com.viasat.burroughs.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;

public class IncludeKey<R extends ConnectRecord<R>> implements Transformation<R> {
    private interface ConfigName {
        String FIELD_NAME = "field_name";
        String MULTIPLE = "multiple";
    }

    public static final String OVERVIEW_DOC = "Includes the record key in the output.";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME,
                    ConfigDef.Type.STRING,
                    "KEY",
                    ConfigDef.Importance.HIGH,
                    "Name for key field")
            .define(ConfigName.MULTIPLE,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    ConfigDef.Importance.LOW,
                    "Does the group by contain multiple fields");

    private static final String PURPOSE = "inserting key";

    private String fieldName;
    private boolean multiple;

    @Override
    public R apply(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(record.valueSchema(),
                SchemaBuilder.struct());

        for (Field field: record.valueSchema().fields()) {
            builder.field(field.name(), field.schema());
        }
        if (!multiple) {
            builder.field(fieldName, record.keySchema());
        }
        else {
            String[] fields = fieldName.split(",");
            for (String fname : fields) {
                builder.field(fname.trim(), Schema.STRING_SCHEMA);
            }
        }
        Schema newSchema = builder.build();
        final Struct updatedValue = new Struct(newSchema);
        for (Field field : record.valueSchema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        if (!multiple) {
            updatedValue.put(fieldName, record.key());
        }
        else {
            String keyStr = (String)record.key();
            String[] fields = fieldName.split(",");
            String[] values = keyStr.split("\\|\\+\\|");
            for (int i = 0; i < fields.length; i++) {
                updatedValue.put(fields[i].trim(), values[i]);
            }
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newSchema, updatedValue, record.timestamp());
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
        fieldName = config.getString(ConfigName.FIELD_NAME);
        multiple = config.getBoolean(ConfigName.MULTIPLE);
    }
}
