---
sort: 1
---

# Motivation
<p>Burroughs exists to bridge the gap between kafka streaming and real time data analysis. Despite providing stable data streaming, Kafka streams are often opaque to data analysts.
The predominant methodology for data scientists to interact with Kafka is via consumption hooks, software implements which read data off the
kafka stream and into a usable format, often a relational database management system. Burroughs achieves this real time "mirroring" of data
at the level of abstraction of a standard sql query, removing the vast overhead for data scientists to write and tune esoteric consmumption hooks from scratch.</p>
