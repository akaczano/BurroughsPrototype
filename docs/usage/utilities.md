---
sort: 3
---

# Utilities

While Burroughs primarily exists to perform stream processing, it comes packaged with a handful of utilities that make it more convenient to use.

### Kafka Utilities
#### List Topics
From the Burroughs CLI, topics can be listed using the `.topics` command. From the GUI, simply look at the sidebar to see a topic list.

#### View topic schema
From the Burroughs CLI, the schema for a topic can be viewed by executing `.topic <topicname>`. To do this from the GUI, click on one of the topics in the list.

#### Delete a topic
To remove an extraneous topic, run `.delete <topicname>` from the CLI, or click on a topic in the GUI and select the delete button on the dialog. Be very careful with this.


### Cleanup
While you should always stop every Burroughs query once you are done with it, sometimes you may forget. When this happens, the query infrastructure, which consists of a handful of potentiall resource intensive KsqlDB objects, will be left running without any purpose. To automatically detect and cleanup any of these objects, run the `.cleanup` command from the CLI or click on the vacuum cleaner button in the toolbar of the GUI.

### Saving and executing queries
In order to run a query saved to a file from Burroughs, you can run the `.file <filename> <delimiter>`. Because the Burroughs CLI runs from a Docker container, the file you are loading must exist inside the commmands directory. The delimiter is used to specify the end of a command or query. While you can only run queries, at a time this gives you a way to run Burroughs commands automatically. For instance, you could do have a file like this

```
.table output;
.producer transactions_producer start;
select storer, sum(spend) from transactions group by 1;
```

Executing this file, would automatically set the output table and start the producer before automatically running the query.

The web interface does not support this kind of Burroughs scripting, but it does provide utilities for loading a query from a file and saving queries to a file.

### Debug tools
If you encounter an error and want more information, you can look at more verbose debugging information than the default Burroughs output. From the CLI, you can run `.debug 1` or `.debug 2` to see more information. Debug level 1 shows the Ksql statements being executed and level 2 shows a detailed execution traceback. You can do the same thing from the GUI by selecting the debug level in the drop down box on the console tab. 