#!/bin/sh
docker run --rm -v $(pwd)/datafiles:/datafiles --link broker:broker --link schema-registry:schema-registry --net confluent_default csv_producer $1
