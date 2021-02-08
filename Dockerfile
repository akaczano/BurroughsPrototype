FROM maven:3.6.3-openjdk-15
COPY . /burroughs/src
WORKDIR /burroughs/src
ADD ["/commands", "/commands"]
RUN mvn clean compile assembly:single
CMD ["java", "-cp", "target/burroughs-0.0.1-jar-with-dependencies.jar", "com.viasat.burroughs.App"]
