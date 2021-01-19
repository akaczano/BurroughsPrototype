#!/bin/sh
mvn clean compile assembly:single
docker image build -f Dockerfile -t burroughs:latest .
