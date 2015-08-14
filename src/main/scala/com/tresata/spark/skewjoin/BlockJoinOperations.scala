package com.tresata.spark.skewjoin

import java.util.{ Random => JRandom }
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.Partitioner.defaultPartitioner

class BlockJoinOperations[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {
  // based on blockJoinWithSmaller in scalding. See com.twitter.scalding.JoinAlgorithms
  private def blockCogroup[W](other: RDD[(K, W)], leftReplication: Int, rightReplication: Int, partitioner: Partitioner): RDD[((K, (Int, Int)), (Iterable[V], Iterable[W]))] = {
    assert(leftReplication >= 1, "must specify a positive number for left replication")
    assert(rightReplication >= 1, "must specify a positive number for right replication")
    def getReplication(random: JRandom, replication: Int, otherReplication: Int) : Seq[(Int, Int)] = {
      val rand = random.nextInt(otherReplication)
      (0 until replication).map{ rep => (rand, rep) }
    }
    val rddBlocked = rdd.mapPartitions{ it =>
      val random = new JRandom
      it.flatMap{ kv =>
        getReplication(random, leftReplication, rightReplication).map{ rl => ((kv._1, rl.swap), kv._2)}
      }
    }
    val otherBlocked = other.mapPartitions{ it =>
      val random = new JRandom
      it.flatMap{ kv =>
        getReplication(random, rightReplication, leftReplication).map{ lr => ((kv._1, lr), kv._2)}
      }
    }
    rddBlocked.cogroup(otherBlocked, partitioner)
  }
  
  /**
  * Same as join, but uses a block join, otherwise known as a replicate fragment join.
  * This is useful in cases where the data has extreme skew.
  * The input params leftReplication and rightReplication control the replication of the left
  * (this rdd) and right (other rdd) respectively.
  */
  def blockJoin[W](other: RDD[(K, W)], leftReplication: Int, rightReplication: Int, partitioner: Partitioner): RDD[(K, (V, W))] = {
    blockCogroup(other, leftReplication, rightReplication, partitioner).flatMap{ blockPair =>
      for (v <- blockPair._2._1.iterator; w <- blockPair._2._2.iterator) yield (blockPair._1._1, (v, w))
    }
  }

  /**
  * Same as join, but uses a block join, otherwise known as a replicate fragment join.
  * This is useful in cases where the data has extreme skew.
  * The input params leftReplication and rightReplication control the replication of the left
  * (this rdd) and right (other rdd) respectively.
  */
  def blockJoin[W](other: RDD[(K, W)], leftReplication: Int, rightReplication: Int) : RDD[(K, (V, W))] = {
    blockJoin(other, leftReplication, rightReplication, defaultPartitioner(rdd, other))
  }

  /**
  * Same as leftOuterJoin, but uses a block join, otherwise known as a replicate fragment join.
  * This is useful in cases where the data has extreme skew.
  * The input param rightReplication controls the replication of the right (other rdd).
  */
  def blockLeftOuterJoin[W](other: RDD[(K, W)], rightReplication: Int, partitioner: Partitioner): RDD[(K, (V, Option[W]))] = {
    blockCogroup(other, 1, rightReplication, partitioner).flatMap{
      case ((k, _), (itv, Seq())) => itv.iterator.map(v => (k, (v, None)))
      case ((k, _), (itv, itw)) => for (v <- itv; w <- itw) yield (k, (v, Some(w)))
    }
  }

  /**
  * Same as leftOuterJoin, but uses a block join, otherwise known as a replicate fragment join.
  * This is useful in cases where the data has extreme skew.
  * The input param rightReplication controls the replication of the right (other rdd).
  */
  def blockLeftOuterJoin[W](other: RDD[(K, W)], rightReplication: Int): RDD[(K, (V, Option[W]))] = 
    blockLeftOuterJoin(other, rightReplication, defaultPartitioner(rdd, other))

  /**
  * Same as rightOuterJoin, but uses a block join, otherwise known as a replicate fragment join.
  * This is useful in cases where the data has extreme skew.
  * The input param leftReplication controls the replication of the left (this rdd).
  */
  def blockRightOuterJoin[W](other: RDD[(K, W)], leftReplication: Int, partitioner: Partitioner): RDD[(K, (Option[V], W))] =  {
    blockCogroup(other, leftReplication, 1, partitioner).flatMap{
      case ((k, _), (Seq(), itw)) => itw.iterator.map(w => (k, (None, w)))
      case ((k, _), (itv, itw)) => for (v <- itv; w <- itw) yield (k, (Some(v), w))
    }
  }

  /**
  * Same as rightOuterJoin, but uses a block join, otherwise known as a replicate fragment join.
  * This is useful in cases where the data has extreme skew.
  * The input param leftReplication controls the replication of the left (this rdd).
  */
  def blockRightOuterJoin[W](other: RDD[(K, W)], leftReplication: Int): RDD[(K, (Option[V], W))] =
    blockRightOuterJoin(other, leftReplication, defaultPartitioner(rdd, other)) 
} 
