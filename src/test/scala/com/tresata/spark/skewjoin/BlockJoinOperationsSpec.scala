package com.tresata.spark.skewjoin

import org.scalatest.FunSpec

import com.tresata.spark.skewjoin.Dsl._

class BlockJoinOperationsSpec extends FunSpec {
  lazy val sc = SparkSuite.sc
  
  describe("BlockJoin") {
    it ("should block inner join") {
      val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (2, 2), (3, 1)))
      val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
      val joined = rdd1.blockJoin(rdd2, 5, 5).collect()
      assert(joined.size === 6)
      assert(joined.toSet === Set(
        (1, (1, 'x')),
        (1, (2, 'x')),
        (2, (1, 'y')),
        (2, (1, 'z')),
        (2, (2, 'y')),
        (2, (2, 'z'))
      ))
    }

    it("should block left outer join") {
      val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (2, 2), (3, 1)))
      val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
      val joined = rdd1.blockLeftOuterJoin(rdd2, 5).collect()
      assert(joined.size === 7)
      assert(joined.toSet === Set(
        (1, (1, Some('x'))),
        (1, (2, Some('x'))),
        (2, (1, Some('y'))),
        (2, (1, Some('z'))),
        (2, (2, Some('y'))),
        (2, (2, Some('z'))),
        (3, (1, None))
      ))
    }

    it("should block right outer join") {
      val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (2, 2), (3, 1)))
      val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
      val joined = rdd1.blockRightOuterJoin(rdd2, 5).collect()
      assert(joined.size === 7)
      assert(joined.toSet === Set(
        (1, (Some(1), 'x')),
        (1, (Some(2), 'x')),
        (2, (Some(1), 'y')),
        (2, (Some(1), 'z')),
        (2, (Some(2), 'y')),
        (2, (Some(2), 'z')),
        (4, (None, 'w'))
      ))
    }
  }
}
