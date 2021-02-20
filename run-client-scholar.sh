#!/bin/sh
set -e
docker run  -d \
	--name burroughs_server \
	-v $(pwd)/producer:/producer \
	--network host \
	-e KSQL_HOST=http://172.18.30.60:32428 \
	-e DB_HOST=172.18.30.60:30123 \
	-e DB_PASSWORD=password \
	-e KAFKA_HOST=172.18.30.60:32275 \
	-e SCHEMA_REGISTRY=http://172.18.30.60:32388 \
	burroughs-server
docker run -d \
	--name burroughs_client \
	--network host \
	burroughs-client
echo "The Burroughs server is now running."
echo "Navigate to http://localhost:5000 to access it."
echo "Run ./stop-client.sh to terminate"	
