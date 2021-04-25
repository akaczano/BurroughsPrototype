---
sort: 2
---

# What Can Burroughs Do?
Burroughs maintains real time mirroring links from a kafka stream into a postgres database based on a sql query. 
A Burroughs user can specify the output table with 
```
.table <tablename>
```
From then on real time mirroring links will be created for any given sql query to an output table in the connected database named
<tablename>.

Basic sql queries such as the following are fully supported in Burroughs

```sql
select store_r,
avg(spend) as avg_spend,
max(spend) as max_spend
from transactions
group by 1;
```

```sql
select custid, sum(spend) as TotalSpend, avg(units)
 as AverageUnits from test_data t inner join test_customers c on t.basketnum = c.basketnum group by 1"
```

```sql
select custid, sum(spend) as TotalSpend, count(*) as TotalTransactions 
from test_data t left join test_customers c on t.basketnum = c.basketnum 
group by 1 having count(*) > 2
```


Burroughs also supports common table expressions and multiple joins.

```sql
with transactions3 as (select * from test_data), pairs as select it1.productnum as source_item,
it2.productnum as target_item from test_data it1 inner join transactions3 it2 on it1.basketnum = it2.basketnum and it1.productnum < it2.productnum
where it1.units > 0 and it2.units > 0) select source_item, target_item, count(1) as frequency from pairs group by 1,2 having count(1) > 2
```
