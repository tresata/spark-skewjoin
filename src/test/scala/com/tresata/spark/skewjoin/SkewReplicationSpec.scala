package com.tresata.spark.skewjoin

import org.scalatest.FunSpec

class SkewReplicationSpec extends FunSpec {
  describe("SkewReplication") {
    it("should return correct replications") {
      val replicator1 = DefaultSkewReplication(1)
      val replicator2 = DefaultSkewReplication(1e-6)
      assert(replicator1.getReplications(5, 4, 1) === (1, 1))
      assert(replicator1.getReplications(80, 5, 90) === (5, 80))
      assert(replicator2.getReplications(10, 50, 80) === (1, 1))
    }

    it("should not return replications bigger than numPartitions") {
      val replicator = DefaultSkewReplication(50)
      assert(replicator.getReplications(5, 4, 100) === (100, 100))
      assert(replicator.getReplications(5, 1, 100) === (50, 100))
    }
  }
}
