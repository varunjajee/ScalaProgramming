package org.muks.examples.spark.WordCount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val resourcesPath = getClass.getResource("/word_count.txt") // Load the source data file into RDD
    println(resourcesPath.getPath)

    val sparkContext =
      new SparkContext(
        new SparkConf()
          .setMaster("local")
          .setAppName("JD Word Counter")
      )


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

    println("Filtered only \"dumpty\" word: %s" +  filteredByString.collect().foreach(println))


    /**
      *     Character count with multiple filters
      */
    val charSplitsFlatMap = sparkContext.textFile(resourcesPath.getPath).flatMap(_.split(""))
    charSplitsFlatMap.collect().foreach(println)

    val charCounters = charSplitsFlatMap.map(char => (char, 1)).reduceByKey(_ + _)
        .filter( !_._1.equals("'") )
        .filter( !_._1.equals(" ") )
        .filter( !_._1.equals(".") )

    charCounters.collect().foreach(println)

  }


}
