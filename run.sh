#!/bin/sh

docker run --rm -it --link ksqldb-server:ksqldb-server --net cp-all-in-one_default -e KSQL_HOST=http://ksqldb-server:8088 -e DB_HOST=postgres:5432 -e DB_PASSWORD=password burroughs
