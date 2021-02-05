FROM confluentinc/cp-kafka-connect-base:6.0.1

RUN   confluent-hub install --no-prompt confluentinc/connect-transforms:latest
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
COPY BurroughsSMT.jar /usr/share/java/
