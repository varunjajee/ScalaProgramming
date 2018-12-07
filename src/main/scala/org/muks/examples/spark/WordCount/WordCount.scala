package org.muks.examples.spark.WordCount

import org.apache.spark.{SparkConf, SparkContext}


/**
  *
  * *
  * What is new SparkConf().setMaster("local[*]")
  *- local : Run Spark locally with one worker thread (i.e. no parallelism at all).
  *- local[K] : Run Spark locally with K worker threads (ideally, set this to the number of cores on your machine).
  *- local[K,F] : Run Spark locally with K worker threads and F maxFailures (see spark.task.maxFailures for an explanation of this variable)
  *- local[*] : Run Spark locally with as many worker threads as logical cores on your machine.
  *- local[*,F] : Run Spark locally with as many worker threads as logical cores on your machine and F maxFailures.
  *
  *
  */

object WordCount {

  def main(args: Array[String]): Unit = {

    val resourcesPath = getClass.getResource("/word_count.txt") // Load the source data file into RDD
    println(resourcesPath.getPath)

    /** What is new SparkConf().setMaster("local[*]") */
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Counter")
    val sparkContext = new SparkContext(sparkConf)

    /**
      * Difference between map and flatMap
      *     Map transforms RDD of N to another RDD of same length N on the basis of lambda function
      *     flatMap transforms RDD of N to another RDD of X - flattened on the basis of lambda function
      */
    val flatMapSplits = sparkContext.textFile(resourcesPath.getPath).flatMap(_.split(" "))
    val mapSplits = sparkContext.textFile(resourcesPath.getPath).map(_.split(" "))

    flatMapSplits.collect().foreach(println)
    mapSplits.collect().foreach(println)


    val wordSplitsFlatMap = sparkContext.textFile(resourcesPath.getPath).flatMap(_.split(" "))
    wordSplitsFlatMap.collect().foreach(println)

    val wordCounts = wordSplitsFlatMap.map(word => (word, 1)).reduceByKey(_ + _)

    /**
      * Alternatively,
      * val wordCounts = wordSplitsFlatMap.map( (_, 1) ).reduceByKey(_ + _)
      */
    wordCounts.collect().foreach(println)
    System.out.println(wordCounts.collect().mkString(", "))


    // filter out words with fewer than threshold occurrence
    val threshold = 3
    val filteredByCount = wordCounts.filter(_._2 >= threshold)
    val filteredByString = wordCounts.filter(_._1.equalsIgnoreCase("dumpty"))

    println("Filtered only \"dumpty\" word: %s" + filteredByString.collect().foreach(println))


    /**
      * Character count with multiple filters
      */
    val charSplitsFlatMap = sparkContext.textFile(resourcesPath.getPath).flatMap(_.split(""))
    charSplitsFlatMap.collect().foreach(println)

    val charCounters = charSplitsFlatMap.map(char => (char, 1)).reduceByKey(_ + _)
      .filter(!_._1.equals("'"))
      .filter(!_._1.equals(" "))
      .filter(!_._1.equals("."))

    charCounters.collect().foreach(println)

  }

}
