if [ $1 = "local" ]; then
	export KSQL_HOST=http://localhost:8088
	export DB_HOST=localhost:5432
	export KAFKA_HOST=localhost:9092
	export DB_PASSWORD=password
	export SCHEMA_REGISTRY=http://localhost:8081
	export CONNECTOR_DB=postgres:5432
	docker cp ./producer/datafiles/transactions.csv postgres:/
	docker cp ./producer/datafiles/customers.csv postgres:/
	docker exec postgres psql -U postgres -c "create database burroughs;"
	docker exec postgres psql -U postgres -d burroughs -c "drop table if exists test_data;"
	docker exec postgres psql -U postgres -d burroughs -c \
		"create table test_data(basketnum int, date date, productnum varchar, spend decimal, units decimal, storer varchar);"
	docker exec postgres psql -U postgres -d burroughs -c "drop table if exists test_customers;"
	docker exec postgres psql -U postgres -d burroughs -c "create table test_customers(basketnum int, custid int);"
	docker exec postgres psql -U postgres -d burroughs -c \
		"\copy test_data(basketnum, date, productnum, spend, units, storer) from /transactions.csv DELIMITER ',' CSV HEADER;"
	docker exec postgres psql -U postgres -d burroughs -c \
		"\copy test_customers(basketnum, custid) from /customers.csv DELIMITER ',' CSV HEADER;"
else
	export KSQL_HOST=http://172.18.30.60:32428
	export DB_HOST=172.18.30.60:30123
	export DB_PASSWORD=password
	export KAFKA_HOST=172.18.30.60:32275
	export SCHEMA_REGISTRY=http://172.18.30.60:32388
fi
export PRODUCER_PATH=src/test/producer
mvn -Dtest=com.viasat.burroughs.BasicQueryTest test
mvn -Dtest=com.viasat.burroughs.ValidationTest test
