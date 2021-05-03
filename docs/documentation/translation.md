---
sort: 2
---

# Query Translation
### <ins>Methodology</ins>
### Summary of the workflow
- Validate SQL query
- Parse the SQL query into SQL clauses
- Translate parsed query
### Translation Process
1. Process common table expressions
2. Create streams: find all of the topics the query depends on (recursively) and create KsqlDB stream for it
   <br>a. topic
   <br>b. subquery
3. Translate joins: Add a window clause to each join
4. Translate functions:
   <br>a. `group_concat` maps to collect_set or collect_list
   <br>b. `cast(_ as Date)` maps to `stringtodate` or datetostring` 
5. Identifiers and aliases
   <br>ex: transactions.storer replace with burroughs_transactions.storer
6. Group by
   <br>a. Replace integers with the field name
   <br>b. Add SMT that extracts the group by from the record key
   