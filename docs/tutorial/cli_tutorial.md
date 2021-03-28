---
sort: 2
---

# Using the Burroughs CLI

This tutorial covers using the Burroughs CLI to do some simple query processing. If you don't have Burroughs running yet, you may want to look at the [quick start guide](/tutorial/quick_start) first.

### Getting Some Test Data
We are now ready to start writing queries, the only problem is we don't currently have any Kafka topics to work with. To solve this, Burroughs provides a built in producer utility to enable users to quickly get some test data to work with. It also ships with a couple of preconfigured producers and associated datasets. To produce some test data to work with, do the following:

`.producer transactions_producer start`

This will produce 100,000 rows of data from a csv file containing transactions data. Once that is done, you can use `.topics` to see that the transactions topic is now present and `.topic transactions` to see the schema for that topic. 

Notice that all of the commands I have been telling you to run start with a `.`. This is because Burroughs by default expects the user to enter a SQL query, but if the input starts with a period, it will interpret it as one of a dozen special commands. To see the full list of special commands and what they do, you can use `.help`.

### Your First Query
Alright, now we can actually start to do some stream processing. First, we need to tell Burroughs where to put the output data, in particular the table name:

`.table output`

Now we can expect the results of the query, if successful, to be in the table called "output" in the Burroughs database.

Lets say we want to know the total amount spent in each of the 4 stores (west, south, east, and central). The SQL would look like this:

```sql
select storer, sum(spend) as Total from transactions group by storer;
```

How do you get Burroughs to execute this? Just type it in:

![]({{ '/assets/images/running_a_query.png' | relative_url }})

As you can see above, the `.status` command will show you some statistics about the currently executing query and give an estimate for how much of the backlog has been processed.

### Viewing Results

At this point, you are probably thinking: "That's great and all, but where are my query results?" They are exactly where you told Burroughs to put them: in the output database. To verify this, open up a second terminal window or tab and do the following:

`docker exec -it postgres psql -U postgres burroughs`

This will get you a SQL interface into the output database, where you can see if everything is working properly. If so, you should be able to do this:

![]({{ '/assets/images/viewing_results.png' | relative_url }})

There is the result of our query, just like we asked. You can now do anything with the results that you can do with a relational database table. As more records are produced to the transactions topic, the new data will be processed and the output table updated accordingly. 

You may also have noticed that there is an additional 'rowkey' field in the results. This is something added by KsqlDB to ensure that every record has a unique key on which to perform an upsert. Most of the time, it will be the same as your group by field.

### Stopping Execution
The actual stream processing mechanism consists of a set of KsqlDB objects (streams, tables, and connectors) that will continue to run and use resources until they are removed. To stop the query execution and destroy this infrastructure, simply run `.stop`. Note that unless you include the `keep-table` flag, this will also remove the output table in the database.

If you already closed your Burroughs CLI, you can no longer run `.stop`, but do not despair. You can simply start up a new Burroughs CLI and run `.cleanup` this will destroy all Burroughs query objects that haven't been reclaimed.

### What's Next?
If you found the CLI experience to be unfriendly, you may want to checkout [The Burroughs Browser Interface]({{ '/tutorial/gui' | relative_url }}).

Ready to connect Burroughs to your own Kafka environment? Have a look at [System Configuration]({{ '/tutorial/config' | relative_url }}).

Want to learn more about what is going on under the hood? Head over to the [documentation section]({{ '/documentation' | relative_url }})