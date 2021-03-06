# Burroughs Developer Notes

## Table of Contents
- [Burroughs Developer Notes](#burroughs-developer-notes)
  - [Table of Contents](#table-of-contents)
  - [Background and Dependencies](#background-and-dependencies)
    - [REST APIs](#rest-apis)
      - [Requests](#requests)
      - [Responses](#responses)
      - [KsqlDB REST API Documentation](#ksqldb-rest-api-documentation)
      - [Prototyping with PostMan](#prototyping-with-postman)
      - [Code Examples](#code-examples)
    - [Gson](#gson)
      - [Serialization](#serialization)
      - [Deserialization](#deserialization)
    - [Apache Calcite](#apache-calcite)
      - [Query Parsing](#query-parsing)
      - [The Object Model](#the-object-model)
    - [JLine](#jline)
    - [JDBC](#jdbc)
    - [Single Message Transforms](#single-message-transforms)
    - [Advanced Java Syntax](#advanced-java-syntax)
  - [Application Overview](#application-overview)
    - [Command Handling](#command-handling)
    - [Query Validation](#query-validation)
    - [Query Execution](#query-execution)
    - [Query Status](#query-status)
    - [Query Disposal](#query-disposal)
  - [Development Workflow](#development-workflow)
    - [Git basics](#git-basics)
    - [Branching and Pull Requests](#branching-and-pull-requests)
    - [Issues](#issues)
## Background and Dependencies
### REST APIs
A REST API is an interface for connection applications together over HTTP that follows a certain architecture. REST APIs make use of the standard array of HTTP methods (GET, POST, PATCH, PUT, DELETE, etc.) and send and receive JSON data. All of the interactions between Burroughs and KsqlDB are done through REST API.
#### Requests
A simple REST request is made up of the following components:
- A URL that identifies the server to send the request to
- The HTTP method to use (mostly GET or POST in our case)
- A body if applicable (GET requests can't have bodies). The request body is always going to be JSON encoded.
- Headers which are used to specify authentication or additional metadata. The ksqlBD REST API doesn't use very many request headers. 
- Query parameters or path parameters that get embedded in the URL
#### Responses
A REST response will usually have a body (also JSON) containing the response data as well as a status code which is usually a 3-digit number. Responses in the 200s are good, 400s and 500s are bad.

#### KsqlDB REST API Documentation
The full KsqlDB REST API documentation can be found [here](https://docs.ksqldb.io/en/latest/developer-guide/api/)

#### Prototyping with PostMan
While the docs demonstrate using the `curl` command to test the REST API, doing so is difficult. Instead, you can use [PostMan](https://www.postman.com/) to test the different endpoints. 

#### Code Examples
Below is an example of sending a REST request using the Java HttpClient API.
```java
// Create a new HTTP client
HttpClient client = HttpClient.newHttpClient();
// Build a request
// hostname is a String containing the url
// The method is POST
// raw is a JSON string that encodes the body  
HttpRequest request = HttpRequest.newBuilder(URI.create(hostname))
    .method("POST", HttpRequest.BodyPublishers.ofString(raw))
    .build();
// Send the request and synchronously get back a response
HttpResponse<String> response = client.send(request,    
        HttpResponse.BodyHandlers.ofString());
// response.body() returns the JSON string encoding the response body
```

### Gson
From the previous section, it is clear the the Burroughs application needs to be able to send and receive lots of JSON data in order to interact with KsqlDB. The problem with this is that generating and parsing JSON strings is a lot of hassle and not particulary necessary. The Gson library provides the ability to convert Java objects to JSON strings and vice versa. The docs can be found [here](https://sites.google.com/site/gson/gson-user-guide).

#### Serialization
 Consider the Java class below:
```java
public class Example {
    @SerializedName("animal_type")
    private String animalType = "Giraffe";

    private int quantity = 10;
    private transient double time = 77.7;
}
```
Running the following line of code would transform an Example object into a JSON string.
```java
String json = new Gson().toJson(new Example());
```
The variable json would then contain the following string:
```json
{"animal_type": "Giraffe", "quantity": 10}
```

A couple of things to note:
1. The field `animalType` was renamed to `animal_type`: its serialized name
2. The field time was not included because it was marked as transient, the standard Java keyword for marking fields that don't get serialized.

#### Deserialization
To get the Java object back, you could do the following:
```java
Example thing = new Gson().fromJson(json, Example.class);
```

Specifying the class will result in a strongly typed deserialization. If the class is not specified, you will most likely get back a JsonObject object;

### Apache Calcite
In order to perform translation, Burroughs needs to be able to analyze and modify SQL queries. Instead of parsing raw SQL strings, we use the Apache Calcite library which provides a SQL parser and object model. The Apache Calcite docs can be found [here](https://calcite.apache.org/docs/reference.html) and the JavaDoc, which contains the full list of classes, [here](https://javadoc.io/doc/org.apache.calcite/calcite-core/latest/index.html).

#### Query Parsing
A query can be parsed using the following code:
```java
String query = "select c.custid, sum(spend) " +
    "from transactions t " +
    "left join customers c " +
    "on t.basketnum = c.basketnum" +
    " left join products p " +
    "on t.productnum = p.productnum";
SqlParser.Config config = SqlParser.configBuilder()
    .setParserFactory(SqlParserImpl.FACTORY)
    .setConformance(SqlConformanceEnum.BABEL)
    .build();
SqlNode node = SqlParser.create(query, config).parseQuery();
```
#### The Object Model
Every SQL component inherits from the SqlNode class. Some examples of subclasses are:
- SqlSelect
- SqlOrderBy
- SqlJoin
- SqlWith
- SqlBasicCall
- SqlIdentifier
- SqlNumericLiteral

Using the object model makes it really easy to extract different pieces of the query. For instance:
```java
SqlSelect selectNode = (SqlSelect)node;
node.getFrom(); // Returns the from section
node.getSelectList(); // Returns a list of identifiers
node.getGroup(); // Returns the group by clause
((SqlJoin)node.getFrom()).getLeft(); // Returns the left side of the join
```
See the api docs for the full list of methods.


### JLine
In order to provide a CLI, Burroughs needs to be able to get user input from the console. Of course, Java provides built in methods of doing this the most common being something like the following:
```java
Scanner scanner = new Scanner(System.in);
String text = scanner.nextLine();
```
While this does work there are some problems with it:
- The scanner input doesn't usually allow for the use of using the right and left arrows to move through the text you're typing.
- There is no good way to use the up arrow to restore the last command.
- Copy-paste is unlikely to work
- Clearing the screen is hard to manage

Instead, Burroughs uses [JLine](https://github.com/jline/jline3), a Java library designed to provide REPL (Read, eval, print loop) terminals. This is the same library that KsqlDB uses to provide its CLI. JLine uses native key listeners to intercept certain keys like arrow keys, Ctr-C, Ctrl-D, Ctrl-L, etc. The following is excerpted from the Burroughs main method:

```java
// Create a JLine terminal
Terminal terminal = TerminalBuilder.terminal();
LineReader lineReader = LineReaderBuilder.builder()
        .terminal(terminal)
        .build();
// The prompt that is shown when expecting user input
String prompt = "sql>";
while (true) {
    String line = null;
    try {
        line = lineReader.readLine(prompt);
    } catch (UserInterruptException e) {
        // This will execute when Ctrl+C is pressed
        continue;
    } catch (EndOfFileException e) {
        // This will execute when Ctrl+D is pressed
        burroughs.dispose();
        return;
    }
    burroughs.handleCommand(line);
}
```
This simple loop creates a much more professional and user friendly terminal than standard Java utilities.

### JDBC
While most of the database interaction is handled from ksqlDB through the use of Kafk Connect, there are a handful of cases where Burroughs needs to able to interface directly with a relational database includeing:
- Dropping the output table when a query is stopped
- Creating the `burroughs` database
- Producing data from a database table

To accomplish this, we make use of the widely used JDBC library. 

To connect to a database you will need a connection string. This takes the following form:
```
jdbc:postgresql://hostname/database
```

You also need a username and password, stored in a java.util.Properties object. The following example establishes connection to a database.

```java
String conString = "jdbc://postgresql://localhost:5432/burroughs"
Properties props = new Properties();
props.put("user", "postgres");
props.put("password", "password");
try {
  Connection conn = DriverManager.getConnection(conString, props);
}
catch (SQLException e) {
  e.printStackTrace();
}
```

Pretty much all JDBC operations can throw a SQLException, so be prepared to handle those.

Once you have a connection, you can query the database:

```java
Statement statement = connection.createStatement();
ResultSet results = statement.execute("SELECT * FROM things;");
```

The result set can be iterated over using `next()`, `isLast()` and other methods. The result set also as a variety of getters to extract fields. For instance, if `things` looked like this:
| Date | Name | Quantity |
| ---- | ---- | -------- |
| 2021-01-18 | Aidan | 4 |
| ... | ... | ... |

I could do the following to extract the fields:

```java
results.next(); // Navigates to the first row
Date date = results.getDate(0); // 2021-01-18
String name = results.getDate(1); // Aidan
int quantity = results.getInt(2); // 4
results.next(); // Move on to the next row
```


The JDBC docs, which contain much more detail, can be found [here](https://docs.oracle.com/javase/tutorial/jdbc/basics/index.html).

### Single Message Transforms
Since Burroughs doesn't do any stream processing itself, we mostly have to rely on the existing features provided by KsqlDB. That being said, KsqlDB does provide a mechanism for performing simple manipulations on data flowing throw a connector in the form of Single Message Transforms (SMTs). There is an informative article on these [here](https://www.confluent.io/blog/kafka-connect-single-message-transformation-tutorial-with-examples/) in addition to the full list of transforms which can be found [here](https://docs.confluent.io/platform/current/connect/transforms/overview.html).

Essentially, transformations will come from one of 3 sources:
- Built in to Kafka connect 
- Made by confluent
- Written by you

In the second two cases the default Confluent Kafka Connect image must be modified. To do this:
1. Start with the cp-kafka-connect-base image.
2. Use the commmand `confluent-hub install --no-prompt` to install any Confluent transforms you need.
3. Copy over any custom transformations in the form of a compiled JAR file. 

For an example of this, see the Dockerfile in the Confluent directory.

## Application Overview

### Command Handling
### Query Validation
### Query Execution
### Query Status
### Query Disposal
## Development Workflow
### Git basics
### Branching and Pull Requests
### Issues
