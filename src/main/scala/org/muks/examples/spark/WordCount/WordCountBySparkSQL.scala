package org.muks.examples.spark.WordCount

object WordCountBySparkSQL {

  def main(args: Array[String]): Unit = {
//    import spark.implicits._
//
//    val dfsFilename = "/input/humpty.txt"
//    val readFileDS = spark.sqlContext.read.textFile(dfsFilename)
//    val wordsDS = readFileDS.flatMap(_.split(" ")).as[String]
//    wordsDS.createOrReplaceTempView("WORDS")
//    val wcounts5 = spark.sql("SELECT Value, COUNT(Value) FROM WORDS WHERE Value ='Humpty' OR Value ='Dumpty' GROUP BY Value")
//    wcounts5.show
  }

}
