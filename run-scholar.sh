#!/bin/sh
export KSQL_HOST=http://172.18.30.60:32428
export DB_HOST=172.18.30.60:30123
export DB_PASSWORD=password
export KAFKA_HOST=172.18.30.60:32275
export SCHEMA_REGISTRY=http://172.18.30.60:32388
java -cp  target/burroughs-0.0.1-jar-with-dependencies.jar com.viasat.burroughs.App
