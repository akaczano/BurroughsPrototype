cd ../SingleMessageTransforms
mvn clean package
cd ../Confluent
cp ../SingleMessageTransforms/target/BurroughsSMT-1.0-SNAPSHOT.jar ./BurroughsSMT.jar
docker image build -t customconnect:latest .
