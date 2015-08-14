package com.tresata.spark.skewjoin

import java.util.{ Random => JRandom }
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner
import org.apache.spark.Partitioner.defaultPartitioner

import com.twitter.algebird.{ CMS, CMSHasher, CMSMonoid }

case class CMSParams(eps: Double = 0.005, delta: Double = 1e-8, seed: Int = 1) {
  def getCMSMonoid[K: Ordering: CMSHasher]: CMSMonoid[K] = CMS.monoid[K](eps, delta, seed)
}

class SkewJoinOperations[K: ClassTag: Ordering: CMSHasher, V: ClassTag](rdd: RDD[(K, V)]) extends Serializable {
  private def getReplicationFactors(random: JRandom, replication: Int, otherReplication: Int): Seq[(Int, Int)] = {
    require(replication > 0 && otherReplication > 0, "replication must be positive")
    val rand = random.nextInt(otherReplication)
    (0 until replication).map(rep => (rand, rep))
  }
  
  private def createRddCMS[K](rdd: RDD[K], cmsMonoid: CMSMonoid[K]): CMS[K] =
    rdd.map(k => cmsMonoid.create(k)).reduce(cmsMonoid.plus(_, _))

  def skewCogroup[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner,
    skewReplication: SkewReplication = DefaultSkewReplication(), cmsParams: CMSParams = CMSParams()): RDD[(K, (Iterable[V], Iterable[W]))] = {
    val numPartitions = partitioner.numPartitions
    val broadcastedLeftCMS = rdd.sparkContext.broadcast(createRddCMS[K](rdd.keys, cmsParams.getCMSMonoid[K]))
    val broadcastedRightCMS = rdd.sparkContext.broadcast(createRddCMS[K](other.keys, cmsParams.getCMSMonoid[K]))
    
    val rddSkewed = rdd.mapPartitions{ it =>
      val random = new JRandom
      it.flatMap{ kv => 
        val (leftReplication, rightReplication) = skewReplication.getReplications(
          broadcastedLeftCMS.value.frequency(kv._1).estimate,
          broadcastedRightCMS.value.frequency(kv._1).estimate,
          numPartitions)
        getReplicationFactors(random, leftReplication, rightReplication).map(rl =>((kv._1, rl.swap), kv._2))
      }
    }
    
    val otherSkewed = other.mapPartitions{ it =>
      val random = new JRandom
      it.flatMap{ kv => 
        val (leftReplication, rightReplication) = skewReplication.getReplications(
          broadcastedLeftCMS.value.frequency(kv._1).estimate,
          broadcastedRightCMS.value.frequency(kv._1).estimate,
          numPartitions)
        getReplicationFactors(random, rightReplication, leftReplication).map(lr => ((kv._1, lr), kv._2))
      }
    }

    rddSkewed.cogroup(otherSkewed, partitioner).map(kv => (kv._1._1, kv._2))
  }

  def skewCogroup[W: ClassTag](other: RDD[(K, W)]): RDD[(K, (Iterable[V], Iterable[W]))] =
    skewCogroup(other, defaultPartitioner(rdd, other))

  def skewJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner,
    skewReplication: SkewReplication = DefaultSkewReplication(), cmsParams: CMSParams = CMSParams()): RDD[(K, (V, W))] =
    skewCogroup(other, partitioner, skewReplication, cmsParams).flatMap{ blockPair =>
      for (v <- blockPair._2._1.iterator; w <- blockPair._2._2.iterator) yield
        (blockPair._1, (v, w))
    }

  def skewJoin[W: ClassTag](other: RDD[(K, W)]): RDD[(K, (V, W))] =
    skewJoin(other, defaultPartitioner(rdd, other))

  def skewLeftOuterJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner,
    skewReplication: SkewReplication = DefaultSkewReplication(), cmsParams: CMSParams = CMSParams()): RDD[(K, (V, Option[W]))] =
    skewCogroup(other, partitioner, RightReplication(skewReplication), cmsParams).flatMap{
      case (k, (itv, Seq())) => itv.iterator.map(v => (k, (v, None)))
      case (k, (itv, itw)) => for (v <- itv; w <- itw) yield (k, (v, Some(w)))
    }

  def skewLeftOuterJoin[W: ClassTag](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] =
    skewLeftOuterJoin(other, defaultPartitioner(rdd, other))

  def skewRightOuterJoin[W: ClassTag](other: RDD[(K, W)], partitioner: Partitioner,
    skewReplication: SkewReplication = DefaultSkewReplication(), cmsParams: CMSParams = CMSParams()): RDD[(K, (Option[V], W))] =
    skewCogroup(other, partitioner, LeftReplication(skewReplication), cmsParams).flatMap{
      case (k, (Seq(), itw)) => itw.iterator.map(w => (k, (None, w)))
      case (k, (itv, itw)) => for (v <- itv; w <- itw) yield (k, (Some(v), w))
    }
  
  def skewRightOuterJoin[W: ClassTag](other: RDD[(K, W)]): RDD[(K, (Option[V], W))] =
    skewRightOuterJoin(other, defaultPartitioner(rdd, other))
}

trait Dsl {
  implicit def rddToSkewJoinOperations_e94qoy3tnt[K: ClassTag: Ordering: CMSHasher, V: ClassTag](rdd: RDD[(K, V)]): SkewJoinOperations[K, V] = new SkewJoinOperations(rdd)
  implicit def rddToBlockJoinOperations_7IaIe6dkih[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]): BlockJoinOperations[K, V] = new BlockJoinOperations(rdd)
}

object Dsl extends Dsl
