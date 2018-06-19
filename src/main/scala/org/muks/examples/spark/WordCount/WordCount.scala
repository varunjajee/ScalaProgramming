package org.muks.examples.spark.WordCount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    println("Scala, word count example.")
    val inputFilePath = "/Users/mukthara/Data/git/personal/SparkProgramming/src/main/resources/word_count.txt"

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("JD Word Counter"))

    // read in text file and split each document into words
    val tokenized =
      sc.textFile(inputFilePath)
        .flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)


    //    // filter out words with fewer than threshold occurrences
    //    val filtered = wordCounts.filter(_._2 >= threshold)
    //    // count characters
    //    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
    //
    //    System.out.println(charCounts.collect().mkString(", "))


    val counts =
      tokenized
        .map(word => (word, 1))
        .reduceByKey(_ + _)

    System.out.println(counts.collect().mkString(", "))
  }


//  private[examples] def delete = {
//    if (file.isDirectory) Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
//    file.delete
//  }


}
