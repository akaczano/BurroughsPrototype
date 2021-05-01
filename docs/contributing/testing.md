---
sort: 2
---

# Unit Testing
In order to maintain consistency and robustness, Burroughs has an ever-growing set of unit tests which can be run using the `test.sh` script or individually using the JUnit tool of your choice. Running the `test.sh` script will produce the data all of the tests rely on and also copy it into the database. This allows the tests to run a query using Burroughs and run the same SQL directly in Postgres and compare the results. 

There are 3 main sets of tests:
- **ValidationTests**: These make sure that Burroughs query validation always returns the correct result.
- **BasicQueryTests**: These test a set of simple, fast querys
- **AdvancedQueryTests**: This set contains some more complicated queries that take a long time to complete and therefore aren't run quite as frequently.