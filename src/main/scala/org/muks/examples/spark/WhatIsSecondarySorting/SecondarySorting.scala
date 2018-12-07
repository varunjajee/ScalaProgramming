package org.muks.examples.spark.WhatIsSecondarySorting

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * Reference link:- https://dzone.com/articles/secondary-sorting-in-spark
  */

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

    // only done locally for demo purposes, usually write out to HDFS
    keyedDataSorted.collect().foreach(println)
  }


  case class FlightKey(airLineId: String, arrivalAirPortId: Int, arrivalDelay: Double)


  def createKeyValueTuple(data: Array[String]): (FlightKey, List[String]) = {
    (createKey(data), listData(data))
  }


  def createKey(data: Array[String]): FlightKey = {
    //val delay double = 0.0
    FlightKey(data(4), data(9).toInt,  data(21).toDouble)
  }

  def listData(data: Array[String]): List[String] = {
    List(data(3), data(6), data(7), data(10))
  }


  /**
    * data repartitioner
    * @param partitions
    */
  class AirlineFlightPartitioner(partitions: Int) extends Partitioner {
    /** entry criteria or assert or base case as in recursion */
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


  /**
    *

  val DAY_OF_MONTH = 0
  val DAY_OF_WEEK = 1
  val FL_DATE = 2
  val UNIQUE_CARRIER = 3
  val CARRIER = 4
  val ORIGIN_AIRPORT_ID = 5
  val ORIGIN_CITY_MARKET_ID = 6
  val ORIGIN_STATE_ABR = 7
  val DEST_AIRPORT_ID = 8
  val DEST_CITY_MARKET_ID = 9
  val DEST_STATE_ABR = 10
  val CRS_DEP_TIME = 11
  val DEP_TIME = 12
  val DEP_DELAY_NEW = 13
  val TAXI_OUT = 14
  val WHEELS_OFF = 15
  val WHEELS_ON = 16
  val TAXI_IN = 17
  val CRS_ARR_TIME = 18
  val ARR_TIME = 19
  val ARR_DELAY = 20
  val CANCELLED = 21
  val CANCELLATION_CODE = 22
val DIVERTED = 23

    */
}
