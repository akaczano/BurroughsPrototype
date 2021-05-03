---
sort: 4
---

# System Configuration


### Environment Variables
Connections between Burroughs and the other components of the stream processing pipeline, Kafka, KsqlDB, and PostgreSQL, are defined by a handful of environment variables. Not all of these variables are required, as many of them have reasonable defaults. If you use Burroughs with the packaged version of the Confluent Platform as described in the previous section, you shouldn't need to change anything. The below table provides a complete list.

| Variable | Description |
| -------- | ----------- |
| KSQL_HOST | The hostname and port number for the ksqldb server. | 
| DB_HOST | The hostname and port for the PostgreSQL database. |
| DB_USER | The database user to provide when connecting. |
| DB_PASSWORD | The database password |
| DATABASE | The database to use. The default is burroughs. |
| KAFKA_HOST | The hostname and port of the Kafka Broker to use. |
| CONNECTOR_DB | The hostname and port for the PostgreSQL to provide to the ksqlDB sink connectors. This will likely be the same as DB_HOST, but it needs to be resolvable from within the Kafka Connect container.
| SCHEMA_REGISTRY | The URL of the AVRO schema registry to use. This is only necessary if you plan on using the embedded producer utility. |
| PRODUCER_PATH | The absolute path to the directory containing the producers.json file |

### Up Next
If you are interested in learning how to setup and configure embedded producers within Burroughs for testing, head over to the [producers section]({{ '/tutorial/producers' | relative_url }}). If you already have all the data you need, move on to the [usage section]({{ '/usage' | relative_url }}) to learn about the full range of SQL Burroughs supports and see some examples of different use cases.