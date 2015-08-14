package com.tresata.spark.skewjoin

import scala.math.{ min, max }
import org.slf4j.LoggerFactory

trait SkewReplication extends Serializable {
  def getReplications(leftCount: Long, rightCount: Long, numPartitions: Int): (Int, Int)
}

case class DefaultSkewReplication(replicationFactor: Double = 1e-2) extends SkewReplication {
  override def getReplications(leftCount: Long, rightCount: Long, numPartitions: Int): (Int, Int) = (
    max(min((rightCount * replicationFactor).toInt, numPartitions), 1),
    max(min((leftCount * replicationFactor).toInt, numPartitions), 1)
  )
}

private case class RightReplication(skewReplication: SkewReplication) extends SkewReplication {
  override def getReplications(leftCount: Long, rightCount: Long, numPartitions: Int): (Int, Int) = {
    val (left, right) = skewReplication.getReplications(leftCount, rightCount, numPartitions)
    (1, max(min(left * right, numPartitions), 1))
    //(1, right)
  }
}

private case class LeftReplication(skewReplication: SkewReplication) extends SkewReplication {
  override def getReplications(leftCount: Long, rightCount: Long, numPartitions: Int): (Int, Int) = {
    val (left, right) = skewReplication.getReplications(leftCount, rightCount, numPartitions)
    (max(min(left * right, numPartitions), 1), 1)
    //(left, 1)
  }
}

private object LoggingSkewReplication {
  private val log = LoggerFactory.getLogger(getClass)
}

case class LoggingSkewReplication(skewReplication: SkewReplication) extends SkewReplication {
  import LoggingSkewReplication._
  private var maxLeftReplication = 0
  private var maxRightReplication = 0

  override def getReplications(leftCount: Long, rightCount: Long, numPartitions: Int): (Int, Int) = {
    val (left, right) = skewReplication.getReplications(leftCount, rightCount, numPartitions)
    if (left > maxLeftReplication) {
      log.info("new max left replication {}", left)
      maxLeftReplication = left
    }
    if (right > maxRightReplication) {
      log.info("new max right replication {}", right)
      maxRightReplication = right
    }
    (left, right)
  }
}
