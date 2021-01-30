FROM openjdk
COPY ["target/burroughs-0.0.1-jar-with-dependencies.jar", "/program.jar"]
ENTRYPOINT ["java", "-cp", "program.jar", "com.viasat.burroughs.App"]
