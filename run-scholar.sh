#!/bin/sh
docker run --rm -it \
-v $(pwd)/producer:/producer \
-v $(pwd)/commands:/commands \
--network host \
-e KSQL_HOST=http://172.18.30.60:32428 \
-e DB_HOST=172.18.30.60:30123 \
-e DB_PASSWORD=password \
-e KAFKA_HOST=172.18.30.60:32275 \
-e SCHEMA_REGISTRY=http://172.18.30.60:32388 \
burroughs
