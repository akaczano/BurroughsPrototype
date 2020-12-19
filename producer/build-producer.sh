#!/bin/sh
mvn clean compile assembly:single
sudo docker image build -f producer.df -t csv_producer:latest .
