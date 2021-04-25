---
sort: 3
---

# Query Execution
Once Query translation is done, the executor receives a calcite object with the query picked apart.

### Overview of the Execution Process
- 1. Generates a unique ID for the query
- 2. Builds a SimpleQuery object for the query, which stores the information related to the ksqlDB table, postgres sink connector, and corresponding intermediate streams/tables for the query.
- 3. For each with in the query, Burroughs builds an intermediate table/stream, depending on the presence of a group by. 
- 4. Once the outermost query has been reached, Burroughs will execute the query against ksqlDB. 
Since a unique ID is generated for each query, the user can use the `.status` command at any point to check on the current state of the query. 
- 5. Key transforms
- 6. Burroughs builds a sink connector to put the aggregate data received from the query into a relational database table.
- 7. Burroughs executes the destroy process to rid of all objects.

### Status Checking

### Single Message Transforms

### The Destroy Sequence
The destroy process ensures to remove all intermediate and unnecessary objects created from Burroughs, to ensure it is maximizing performance. While the query is being executed, Burroughs will keep track of all built streams/tables so they can be properly dealt with when the destroy sequence is called.
- 1. Drop the postgres sink connector
- 2. Drop the final ksqlDB output table
- 3. Drop all intermediate streams
- 4. Drop all intermediate tables

### Error cases 
If at any point Burroughs reaches an error, it will stop the query and return an error to the user interface, with a short description of the error. The user can use the `.debug {LEVEL}` command to print out more information on what caused the error. Generally, most errors will come from being a limitation of ksqlDB, and Burroughs will return a stacktrace from ksqlDB to portray this.  