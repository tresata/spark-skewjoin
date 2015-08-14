[![Build Status](https://travis-ci.org/tresata/spark-skewjoin.svg?branch=master)](https://travis-ci.org/tresata/spark-skewjoin)

# spark-skewjoin

## Functionality
This library adds the skewJoin operation to RDD[(K, V)] where possible (certain implicit typeclasses are required for K and V).
A skew join is just like a normal join except that keys with large amounts of values are not processed by a single task but instead spread out across many tasks. This is achieved by replicating key-value pairs for one side of the join in such way that they go to multiple tasks, while randomly spreading out key-value pairs for the other side of the join across the same tasks (the actual implementation is symmetrical and can replicate and spread out at the same time for both sides).
A skew join is like a bucket/block join except the replication factor is adjusted per key instead of being fixed.
To be able to pick how much replication a key needs the skew join first estimates counts for all keys on both sides of the join, using a count-min-sketch (CMS) probabilistic data structure. This means the operation will process the RDDs twice:
* once for the approximate count of values per key, an immediate action
* once for the actual join, a RDD transformation

Besides skewJoin this library also adds the blockJoin operation to RDD[(K, V)]. A block join is a skew join with fixed replication factors for both sides of the join. Because the replication factors are fixed no approximate count needs to be done either, avoiding a Spark immediate action for this count.

## Usage
```scala
import com.tresata.spark.skewjoin.Dsl._
val left: RDD[K, V] = ...
val right: RDD[K, W] = ...
val joined1: RDD[K, (V, W)] = left.skewJoin(right)
val joined2: RDD[K, (V, W)] = left.blockJoin(right, 2, 2)
```

## Additional info
The skewJoin method takes in optional arguments:
* partitioner: Partitioner
  This is typical spark partitioner to control the partitioning strategy
* skewReplication: SkewReplication
  The skewReplication is used to pick the replication factors per key given the counts. A custom implementation can be provided by user.
* cmsParams: CMSParams
  This is a simple case class with parameters for the count-min-sketch data structure used in the approximate counting of keys.
  Please see the Algebird documentation for additional info.
Besides skewJoin and blockJoin this library also makes available skewLeftOuterJoin, skewRightOuterJoin, blockLeftOuterJoin and blockRightOuterJoin, but be aware that outer joins restrict the ability to replicate keys on one side of the join and therefore should be avoided if possible.

Currently this library is alpha stage.

Have fun!
Team @ Tresata
