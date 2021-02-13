#!/bin/sh

docker run --rm -it \
-v $(pwd)/producer:/producer \
-v $(pwd)/commands:/commands \
--link ksqldb-server:ksqldb-server \
--net confluent_default \
-e KSQL_HOST=http://ksqldb-server:8088 \
-e DB_HOST=postgres:5432 \
-e DB_PASSWORD=password \
-e KAFKA_HOST=broker:29092 \
-e SCHEMA_REGISTRY=http://schema-registry:8081 \
burroughs
