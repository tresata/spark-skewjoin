package com.tresata.spark.skewjoin

import org.scalatest.FunSpec

import com.tresata.spark.skewjoin.Dsl._

import com.twitter.algebird.CMSHasherImplicits._

import org.apache.spark.Partitioner.defaultPartitioner

case object DummySkewReplication extends SkewReplication {
  override def getReplications(leftCount: Long, rightCount: Long, numPartitions: Int) = (2, 2)
}

class SkewJoinOperationsSpec extends FunSpec {
  lazy val sc = SparkSuite.sc
  lazy val rdd1 = sc.parallelize(Array(1, 1, 2, 3, 4)).map(s => (s, 1)).repartition(2)
  lazy val rdd2 = sc.parallelize(Array(1, 1, 6, 4, 5)).map(s => (s, 2)).repartition(2)

  describe("SkewJoin") {
    it("should inner join two datasets using skewJoin correctly") {
      assert(rdd1.skewJoin(rdd2, defaultPartitioner(rdd1, rdd2), DefaultSkewReplication(1)).sortByKey(true).collect.toList === 
        Seq((1, (1, 2)), (1, (1, 2)), (1, (1, 2)), (1, (1, 2)), (4, (1, 2))))
    }

    it("should left join two datasets using skewLeftOuterJoin correctly") {
      assert(rdd1.skewLeftOuterJoin(rdd2, defaultPartitioner(rdd1, rdd2), DefaultSkewReplication(1)).sortByKey(true).collect.toList ===
        Seq((1, (1, Some(2))), (1, (1, Some(2))), (1, (1, Some(2))), (1, (1, Some(2))), (2, (1, None)), (3, (1, None)), (4, (1, Some(2)))))
    }

    it("should right join two datasets using skewRightOuterJoin correctly") {
      assert(rdd1.skewRightOuterJoin(rdd2, defaultPartitioner(rdd1, rdd2), DefaultSkewReplication(1)).sortByKey(true).collect.toList ===
        Seq((1, (Some(1), 2)), (1, (Some(1), 2)), (1, (Some(1), 2)), (1, (Some(1), 2)), (4, (Some(1), 2)), (5, (None, 2)), (6, (None, 2))))
    }
  }
}
