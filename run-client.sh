#!/bin/sh
set -e
docker run  -d \
	--name burroughs_server \
	-v $(pwd)/producer:/producer \
	-p 5002:5002 \
	--net confluent_default \
	-e KSQL_HOST=http://ksqldb-server:8088 \
	-e DB_HOST=postgres:5432 \
	-e DB_PASSWORD=password \
	-e KAFKA_HOST=broker:29092 \
	-e SCHEMA_REGISTRY=http://schema-registry:8081 \
	-e PRODUCER_PATH=/producer \
	burroughs-server
docker run -d \
	--name burroughs_client \
	-p 5000:5000 \
	burroughs-client
echo "The Burroughs server is now running."
echo "Navigate to http://localhost:5000 to access it."
echo "Run ./stop-client.sh to terminate"	
