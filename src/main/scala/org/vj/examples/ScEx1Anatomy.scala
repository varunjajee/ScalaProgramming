package org.vj.examples

import org.apache.spark.{SparkContext, SparkConf}

object ScEx1Anatomy {
  def main(args: Array[String]) {

    val masterURL = "local[*]"

    val conf = new SparkConf()
      .setAppName("SparkMe Application")
      .setMaster(masterURL)

    val sc = new SparkContext(conf)

    val fileName = util.Try(args(0)).getOrElse("README.md")
    //val fileName = getClass.getResource("/emp.csv") // Load the source data file into RDD//util.Try(args(0)).getOrElse("build.sbt")

    val lines = sc.textFile(fileName).cache()

    val c = lines.count()
    println(s"There are $c lines in $fileName")
  }

}
