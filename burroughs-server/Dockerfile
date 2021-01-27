FROM openjdk:12-oraclelinux7

RUN curl -sL https://rpm.nodesource.com/setup_10.x | bash - && \
	yum install -y npm && \
	yum remove -y nodejs && \
	yum install -y nodejs && \
	npm install -g serve
COPY ./application.properties /
COPY ./target/burroughs-server-0.0.1-SNAPSHOT.jar /program.jar
COPY ./client/burroughs-client /client
COPY ./start-server.sh /start-server.sh

CMD ["/start-server.sh"]

