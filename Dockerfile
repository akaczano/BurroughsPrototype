FROM maven:3.6.3-openjdk-15
COPY src/ /burroughs/src/src/
COPY pom.xml /burroughs/src
WORKDIR /burroughs/src
RUN mvn clean compile assembly:single
CMD ["java", "-cp", "target/burroughs-0.0.1-jar-with-dependencies.jar", "com.viasat.burroughs.client.App"]
