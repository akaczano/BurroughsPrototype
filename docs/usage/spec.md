---
sort: 1
---

# SQL Specification

## Query Structure

In general, a Burroughs query takes the following form:

```sql
[with <with_list>]
select <field_list> 
from <from_clause>
[where <condition>]
group by <field_list>
[having <condition>]
[limit <integer>]
```

The **with_list** consists of an arbitrary number of comma-separated named subqueries.

A **subquery** is a second query (with out a with list) that is enclosed in parentheses and can be named with the `as` keyword.

The **field_list** is a comma-separated list of fields. Fields can be renamed using the `as` keword, wrapped in an arbitrary number of function calls, and identified using the `table_alias.field_name` syntax. In the case of the `group by` clause, the field list can contain integers which reference the position (starting with position 1) of an entry in the select list. 

The **from_clause** can be either a topic name, common table expression name, or an arbitrary number of joins, where each join operates on any combination of topics, subqueries, or common table expressions. The allowed joins are `inner`, `left outer` which can be abbreviated to `left`, and `full outer` which can be abbreviated as `full`. The join condition must be an equality comparison. Any object referenced in the from clause can be aliased using the `as` keyword.

## Limitations

### Group By
First of all, you may have noticed that the group by clause is not optional: each outer-level query must have a `group by`. Subqueries on the other hand, may not have a group by clause, although common table expressions can. When a group by is included within a common table expression, the select list may not include the aggregate functions `min` or `max` and the group by can only contain a single field. 

### Order By
Because Burroughs is a stream processing application and sorting is inherently a blocking operation, Burroughs does not support any kind of `order by`. This also means that any limit clause applied to Burroughs will be non-deterministic.  



## Data Types
Since Burroughs requires all of its data items to be serialized using [Avro](https://avro.apache.org/), the range of data types supported is much smaller than the typical relational database. The full list is as follows:

- INTEGER (32-bit integer)
- BIGINT  (64-bit integer)
- BOOLEAN
- DOUBLE
- STRING

KsqlDB also supports the compound data structures array, struct, and map, but Burroughs does not support these because they don't have obvious equivalents in standard SQL. 

### Dates
More likely than not, you are going to want to work with dates in Burroughs. A date can be encoded as a BIGINT (days since the epoch) and Burroughs provides convenient ways to work with these integer dates and display them. See the [examples section]({{'/usage/examples' | relative_url }}) for the details on how to do this

## Supported Functions

### Aggregate Functions 
Burroughs supports the following SQL aggregate functions:
- `count`
- `sum`
- `avg`
- `min`
- `max`

It also supports the following KsqlDB aggregate functions
- `earliest_by_offset` - returns the value in the column with the smallest offset 
- `latest_by_offset` - return the value in the column with the largest offest

Finally, Burroughs supports one additional aggregate function:

`group_concat([distinct] field [, seperator])`

which will return all of the values or distinct values of a given field concatenated together with the given separator. 

### Scalar Functions
In general, Burroughs supports all of the [standard KsqlDB scalar functions](https://docs.ksqldb.io/en/0.10.2-ksqldb/developer-guide/ksqldb-reference/scalar-functions/) which don't operate on an unsupported type. Burroughs also extends the `cast` function to work with dates.


## Windowing
