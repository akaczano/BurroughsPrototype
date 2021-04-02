---
sort: 3
---

# The Burroughs Browser Interface

This tutorial covers using the Burroughs GUI to do some simple query processing. If you don't have Burroughs running yet, you may want to look at the [quick start guide]({{ '/tutorial/quick_start' | relative_path }}) first.

### A Quick Tour of the UI
Before we get started, I would like to point out a few things about this user interface.

1. In the top right corner of the screen you will see a couple of labels that tell you whether the two most important services (KsqlDB and PostgreSQL) are connected or not. If you see disconnected on either one of them try waiting a few seconds and then pressing the 'Reconnect' button.
2. In the bottom left section of the screen the connection information for the database Burroughs is using is displayed. This may come in handy when you want to use the output data or troubleshoot a connection problem.
3. Above that you will find a list of all of the topics on the connected Kafka broker. You may see 3 topics already there by default, all of which are prefixed with 'docker-connect-'. These contain administrative data for Kafka Connect. For other topics, you can click on the name to view the schema. 
4. In the center of the screen you will find the editor where queries can be entered and executed. The first 2 buttons the toolbar can be used to open and save files containing queries. The next 2 are used to run and terminate queries. The checkbox specifies whether the output data should be deleted upon termination. Finally, the far right button can be used to cleanup any additional resources left over from queries that weren't terminated.
6. The final part of the interface is the tabbed pane at the bottom. Here is a brief description of each tab:
  - **Status**: This is where status and run-time statistics for the currently running query are displayed
  - **Console:** This will show the output trace during connection, execution, and termination. You can also adjust the debug verbosity using a drop down box.
  - **Data:** This provides a minimal SQL interface into the Burroughs output database and a convenient way to view your query results.
  - **Producers:** Here you can manipulate any built in producers you have configured. This is mostly just useful for testing, as Burroughs isn't really designed to be the source of the data you are analyzing.


### Getting Some Test Data
To get some test data to work with, navigate to the producers tab and click the 'start' button under the 'transactions_producer' entry. You should see an entry titled 'transactions' pop up in the topics list. Feel free to click on it to view the schema.

#### A Few More Words on Producers
Producers can be configured to limit the quantity of data produced and the rate at which it is transmitted. Setting a positive value for limit will stop the producer once that many records have been produced. The delay field is the length of a sleep period (in ms) to be inserted between every produced record. You can also pause and resume your producers to simulate a large delay. 

### Running a query
To run a query, you need to do the following
1. Enter a value in the 'Output Table' field. I am partial to simply using the name 'output', but you may want something more meaningful.
2. Write a SQL query in the editor (or open an existing query from a file).
3. Press the execute button (that's the green triangle in the toolbar). 

In my case, I wrote a little query that gives me some summary statistics for each purchase:

```sql
select
    basketnum,
    sum(spend) as TotalSpend,
    avg(units) as AvgUnits,
    count(*) as TransactionCount
from transactions
group by basketnum
```

Here is what executing it should look like:

![]({{ '/assets/images/running_a_query_gui.png' | relative_url }})

As you can see, the status display gives information on the current process rate and the amount of backlog still remaining to be processed.

We can also look at the results from the data tab like so:

![]({{ '/assets/images/viewing_results_gui.png' | relative_url }})

When you're done, simply press the stop button to shut everything done. If you are curious about what that entails, have a look at the console while that's running:

![]({{ '/assets/images/console.png' | relative_url }})


### What's Next?

Ready to connect Burroughs to your own Kafka environment? Have a look at [System Configuration]({{ '/tutorial/config' | relative_url }}).

Want to learn more about what is going on under the hood? Head over to the [documentation section]({{ '/documentation' | relative_url }})