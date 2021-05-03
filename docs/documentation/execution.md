---
sort: 3
---

# Query Execution
Once Query translation is done, the executor receives a calcite object with the query picked apart.

### Overview of the Execution Process
1. Generates a unique ID for the query
2. Builds a SimpleQuery object for the query, which stores the information related to the ksqlDB table, postgres sink connector, and corresponding intermediate streams/tables for the query.
3. For each with in the query, Burroughs builds an intermediate table/stream, depending on the presence of a group by. 
4. Once the outermost query has been reached, Burroughs will execute the query against ksqlDB. 
5. Key transforms
6. Burroughs builds a sink connector to put the aggregate data received from the query into a relational database table.
7. Burroughs executes the destroy process to rid of all objects.

Since a unique ID is generated for each query, the user can use the `.status` command at any point to check on the current state of the query. 

### Status Checking
Burroughs provides an interface for monitoring the status of a query execution. This is done by monitoring of all of the Kafka consumers KsqlDB generates for query processing and tracking their offsets relative to the max topic offset. This method gives the user an idea of how much backlog the query still has to process and how well Burroughs is able to keep up with the live stream. 

### Single Message Transforms
Sometimes, the results that KsqlDB outputs aren't good enough or KsqlDB doesn't support something that Burroughs needs to. In those cases, a good option is to a Single Message Transform (SMT) which allows you to operate on the data as it is being written to the database. The current version of Burroughs uses SMTs for 4 different purposes:

1. When the query contains a having clause, an SMT is used to remove records that don't match the predicate.
2. When a query uses the `group_concat` function, it gets translated to the KsqlDB `collect_set` or `collect_list`. Both of those functions output an Avro list type and a SMT is added to convert that list to a string with the separators inserted
3. When the query contains a limit clause, an SMT is used to remove any new records once the limit is reached.
4. Since the newer versions of KsqlDB don't include the table key within the message value, an SMT is used to extract the field from the message key when the data is synced. If a query has multiple group by fields, KsqlDB will append them together and include the concatenation as the message key. In this case, the SMT will split the key apart and insert both fields into the message.

### The Destroy Sequence
The destroy process ensures to remove all intermediate and unnecessary objects created from Burroughs, to ensure it is maximizing performance. While the query is being executed, Burroughs will keep track of all built streams/tables so they can be properly dealt with when the destroy sequence is called.

1. Drop the postgres sink connector
2. Drop the final ksqlDB output table
3. Drop all intermediate streams
4. Drop all intermediate tables

### Error cases 
If at any point Burroughs reaches an error, it will stop the query and return an error to the user interface, with a short description of the error. The user can use the `.debug {LEVEL}` command to print out more information on what caused the error. Generally, most errors will come from being a limitation of ksqlDB, and Burroughs will return a stacktrace from ksqlDB to portray this.  