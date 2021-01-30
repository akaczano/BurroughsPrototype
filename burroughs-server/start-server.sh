#!/bin/sh
java -jar /program.jar &
cd client
serve -s build
