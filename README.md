# Burroughs Prototype

## Getting Started

Requirements
- Docker
- Maven

### 1. Confluent Platform Setup

The first thing you'll need to do is start up the Confluent platform containers (Zookeeper, Kafka, KsqlDB, etc.) and the PostgreSQL database, all of which are contianed in the Confluent directory.
```bash
cd Confluent
docker-compose up -d
```

Give it a couple seconds and then make sure that everything is running by doing `docker-compose ps`. If anything has exited, just start it again.

### 2. Test Data
To actually make use of Burroughs, you'll want some data to play with. This repository contains a demo CSV file and simple Kafka producer to solve this. To run it do the following:
```bash
cd producer
./build-producer.sh
./start-producer.sh 1000
```
The above code will produce 1000 records to a Kafka topic for testing. You may specify any limit you like up to the 100,000 records that are in the file.

### 3. Running Burroughs
To build and run burroughs do the following (from the root directory).
```bash
./build.sh
./run.sh
```
If you setup everything successfully, you should see something like the following.
![screenshot](images/landing.png)