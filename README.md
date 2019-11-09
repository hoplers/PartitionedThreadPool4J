# PartitionedThreadPool4J

An implmenetaiton of a thread pool that allows tasks to be executed in partitions.
## Concept
When a task is submitted to the partition thread pool, it is assigned to one of the partitions according to it's partitioning key - either by using a provided partitioning strategy, or by using the default one.
Each partition has its own queue and one thread for execution, allowing strong ordering guarantees inside a partition.

## Usage

**Java version 8 or above is required to use this library.**

Add dependency to maven -
```
  <groupId>io.github.hoplers</groupId>
  <artifactId>PartitionedThreadPoolExecutor</artifactId>
  <version>0.0.3</version>
```

Add dependency to gradle -
```
compile group: 'io.github.hoplers', name:'PartitionedThreadPoolExecutor', version: '0.0.3'
```
