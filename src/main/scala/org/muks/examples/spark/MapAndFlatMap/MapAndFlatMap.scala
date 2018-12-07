package org.muks.examples.spark.MapAndFlatMap

import org.apache.spark.{SparkConf, SparkContext}

object MapAndFlatMap {
  def main(args: Array[String]): Unit = {

    val resourcesPath = getClass.getResource("/word_count.txt") // Load the source data file into RDD
    println(resourcesPath.getPath)

    val sparkContext =
      new SparkContext(
        new SparkConf()
          .setMaster("local")
          .setAppName("JD Word Counter")
      )


    /**
      * Difference between map and flatMap
      *
      * Both are transformations in spark.
      *
      * map
      * -> returns "array[string]"
      * -> one-to-one transformation
      *
      * flatMap
      * -> return "string"
      * -> one-to-many transformation
      */
    val wordSplitsMap =
      sparkContext.textFile(resourcesPath.getPath)
      .map(line => line.split(" ")).map(x => x {1}.toLowerCase)

    wordSplitsMap.collect().foreach(println)


    val wordSplitsFlatMap = sparkContext.textFile(resourcesPath.getPath).flatMap(_.split(" "))
    wordSplitsFlatMap.collect().foreach(println)

  }

}
