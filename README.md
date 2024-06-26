# Delta Lake vs. Apache Spark: Understanding ACID Properties

## Authors
- Diptesh Saha
- Samapan Kar
- Uddalak Mukherjee

## Introduction
Delta Lake is an extension to Apache Spark designed to add ACID (Atomicity Consistency Isolation Durability) properties to data lakes, enhancing data integrity and reliability.

## Purpose of Presentation
This project explores how Delta Lake preserves ACID properties, why Apache Spark lacks these properties by default, and the impact of these differences on data processing.

## What is ACID?
- **Atomicity**: Ensures that each transaction is treated as a single unit which either succeeds completely or fails completely.
- **Isolation**: Ensures that the concurrent execution of transactions results in a system state that would be obtained if transactions were executed serially.
- **Consistency**: Ensures that any transaction will bring the database from one valid state to another valid state.
- **Durability**: Ensures that once a transaction has been committed, it will remain so even in the event of a crash, power loss, etc.

## Delta Lake and ACID
Delta Lake, built atop Apache Spark, incorporates several mechanisms to enforce ACID properties within data lakes. These mechanisms include:
- **Transaction Log**: Maintains a log of all data changes ensuring transactions are atomic and durable.
- **Snapshot Isolation**: Enables consistent data views across transactions by maintaining data versions at specific time points.
- **Metadata Management**: Stores essential metadata alongside data to manage consistency and isolation effectively.
- **Compaction & Vacuuming**: Regularly performs data optimization tasks to enhance performance and reclaim storage space.

## Limitations of Apache Spark
- **Lack of Transaction Log**: Without a transaction log, Spark cannot ensure atomic and durable operations.
- **Limited Metadata Management**: Spark has restricted capabilities for managing data consistency and isolation in concurrent environments.
- **No Native Support for Consistent Views**: Spark does not natively support consistent views of data across concurrent transactions, leading to potential inconsistencies.

## Concurrency Control in Delta Lake
Delta Lake opts for optimistic concurrency control over traditional locking mechanisms. This approach minimizes transaction blocking and resolves conflicts at commit time, enhancing both concurrency and system scalability.

## ACID Failures in PySpark
Plain PySpark can exhibit failures in ACID properties during concurrent transactions, such as updates on the same data set. This is illustrated through use cases where operations like updating account balances concurrently without ACID guarantees lead to inconsistent or incorrect outcomes.

## ACID Success in Delta Lake
Delta Lake, by utilizing methods like transaction logs and snapshot isolation, can successfully handle concurrent transactions while ensuring data integrity. This ensures that operations, whether they are concurrent or isolated, faithfully represent the user's intentions and the true state of the data lake.

## Practical Example
A practical example of ACID enforcement in Delta Lake versus failure in PySpark is demonstrated by simulating concurrent bank transactions. In Delta Lake, concurrent updates on account balances are consistent and correct, whereas in PySpark, they can result in data inconsistencies due to the lack of transaction management and isolation.

## Thank You!
