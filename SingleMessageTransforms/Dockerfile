FROM confluentinc/cp-kafka-connect-base:6.0.1

RUN   confluent-hub install --no-prompt confluentinc/connect-transforms:latest &&\
confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest &&\
confluent-hub install --no-prompt confluentinc/kafka-connect-datagen:latest
COPY target/BurroughsSMT-1.0-SNAPSHOT.jar /usr/share/java/
