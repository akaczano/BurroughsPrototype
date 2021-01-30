#!/bin/sh
docker run  -d \
	--name burroughs_server \
	-v $(pwd)/../producer:/producer \
	-p 5000:5000 \
	-p 5002:5002 \
	--net confluent_default \
	-e KSQL_HOST=http://ksqldb-server:8088 \
	-e DB_HOST=postgres:5432 \
	-e DB_PASSWORD=password \
	-e KAFKA_HOST=broker:29092 \
	-e SCHEMA_REGISTRY=http://schema-registry:8081 \
burroughs_server
	
