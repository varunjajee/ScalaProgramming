package org.muks.examples.spark.GroupReduceAggregateCombiner

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Example {

  def main(args: Array[String]): Unit = {
    val sparkContext =
      new SparkContext(
        new SparkConf()
          .setMaster("local")
          .setAppName("JD Word Counter")
      )

    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
    val data = sparkContext.parallelize(keysWithValuesList)
    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    uniqueByKey.collect().foreach(println)



    val initialCount = 0;
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    countByKey.collect().foreach(println)
  }

}
