package org.muks.examples.spark.WhatIsSecondarySorting

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object SecondarySorting {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Emp Dept Assignment").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val resourcesPath = getClass.getResource("/airlinesData.csv") // Load the source data file into RDD
    println(resourcesPath.getPath)

    val rawDataArray = sc.textFile(resourcesPath.getPath).map(line => line.split(","))
    val header = rawDataArray.first() // extract header


    val airlineData = rawDataArray.filter(someLine => (someLine != header)).map(arr => createKeyValueTuple(arr))
    airlineData.take(3).foreach(println)


    val keyedDataSorted = airlineData.repartitionAndSortWithinPartitions(new AirlineFlightPartitioner(1))

    //only done locally for demo purposes, usually write out to HDFS
    keyedDataSorted.collect().foreach(println)


  }


  case class FlightKey(airLineId: String, arrivalAirPortId: Int, arrivalDelay: Double)


  def createKeyValueTuple(data: Array[String]): (FlightKey, List[String]) = {
    (createKey(data), listData(data))
  }


  def createKey(data: Array[String]): FlightKey = {
    val delay double = 0.0

    FlightKey(data(4), data(9).toInt,  )
  }

  def listData(data: Array[String]): List[String] = {
    List(data(3), data(6), data(7), data(10))
  }


  /**
    * data repartitioner
    * @param partitions
    */
  class AirlineFlightPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[FlightKey]
      k.airLineId.hashCode() % numPartitions
    }
  }

  object FlightKey {
    implicit def orderingByIdAirportIdDelay[A <: FlightKey] : Ordering[A] = {
      Ordering.by(fk => (fk.airLineId, fk.arrivalAirPortId, fk.arrivalDelay * -1))
    }
  }

}
