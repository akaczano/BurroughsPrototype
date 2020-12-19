#!/bin/sh
mvn clean compile assembly:single
sudo docker image build -f Dockerfile -t burroughs:latest .
