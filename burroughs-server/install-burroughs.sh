cd ..
mvn clean compile assembly:single
mvn install:install-file \
	-Dfile=target/burroughs-0.0.1-jar-with-dependencies.jar \
	-DgroupId=com.viasat \
	-DartifactId=burroughs \
	-Dversion=0.0.1 \
	-Dpackaging=jar
	-DgeneratePom=true
