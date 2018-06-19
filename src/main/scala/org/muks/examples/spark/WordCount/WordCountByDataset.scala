package org.muks.examples.spark.WordCount

import org.apache.spark.sql.SparkSession

object WordCountByDataset {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()
  }

}
