docker run --rm -v $(pwd):/src -w /src maven:3.6.3-openjdk-15 mvn clean package
docker image build -t customconnect:latest .
