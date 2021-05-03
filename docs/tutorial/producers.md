---
sort: 4
---

# Producers
Burroughs is not designed to be used as a data source, but sometimes it is convenient to be able to quickly produce some data for testing. Producers can be created and configured in the producers.json file in the producer directory, which expects an array of producer objects. For example, the default producer that ships with Burroughs looks like this:
```json
  {
    "name": "transactions_producer",
    "topic": "transactions",    
    "schema": "transaction.avsc",
    "key_field": "StoreR", 
    "data_source": {
      "type": "file",
      "source": {
        "location": "datafiles/transactions.csv",
        "header": true,
        "delimiter": ","
      }
    }
  }
```

The below tables describe all of these properties and their uses. All paths are relative to the producer folder.

| Property | Description | Type |Required | Default |
| -------- | ----------- | ---- | -------- | ------- |
| name | Used to reference the producer when executing commands | String | Yes | None
| topic | The topic to produce the records onto | String | Yes | None |
| schema | The path to the AVRO schema file for this data source | String | Yes | None |
| key_field | The field to use as the message key. Must be defined in the schema file. If none is specified the message value will be used as the key | String | No | None |
| data_source | The data source to pull records from | Object | Yes | None |

The data source object must have a type field which can be either "file" or "database". It must also have a source object whose fields are defined below.

| Property | Description | Type | Data source | Default |
| -------- | ----------- | ---- | ----------- | ------- | 
| location | Path to the data file | String | File | None |
| delimiter | Specifies the delimiter | String | File | , |
| header | Whether or not the file contains a header line which must be skipped | Boolean | File | false |
| hostname | Database hostname | String | Database | Burroughs database host |
| database | Database name | String | Database | Burroughs database name |
| username | Database user | String | Database | Burroughs database user |
| password | Database password | String | Database | Burroughs database password |

Producers can be started using the command:
```burroughs
.producer <name> start [record limit]
```
To see the other producer operations check out the [utilities section]({{ '/usage/utilities' | relative_url }}) or simply run the `.help` command in the CLI.