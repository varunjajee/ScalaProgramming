package org.muks.examples.spark.GroupByAnReduceBy

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object GroupByReduceByAggregateByKey {

  def main(args: Array[String]): Unit = {


    /**
      * Difference between groupByKey and reduceByKey()
      * groupByKey
      * -> should be avoided as it shuffles upright
      * -> might end up spilling over the disk in case of large datasets
      *
      * reduceByKey() - uses combiner internally by spark
      * -> Preferred for large datasets
      * -> First executes the lamda function within the partitions, first reducing, internal within partitions
      * -> then shuffles to reduce by finding out the right partitions of similar keys
      */


    val conf = new SparkConf().setAppName("Emp Dept Assignment").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val resourcesPath = getClass.getResource("/emp.csv") // Load the source data file into RDD
    println(resourcesPath.getPath)

    val empDataRDD = sc.textFile(resourcesPath.getPath)
    println(empDataRDD.foreach(println))

    // Find first record of the file
    val headerLine = empDataRDD.first()
    println("Header line:- " + headerLine)

    // Remove header from RDD
    val empDataRdd = empDataRDD.filter(someLine => !someLine.equals(headerLine))




    /**
      *   === groupByKey ===
    * USing groupBy and takeOrdered( - over reverse ordering - )
    */
    val salaryWithEmployeeName = empDataRdd.map { x => x.split(',') }.map { x => (x(5).toDouble, x(1)) }
    val maxSalaryGroupByKey = salaryWithEmployeeName.groupByKey.takeOrdered(2)(Ordering[Double].reverse.on(_._1))
    print("Max Salary using groupByKey.takeOrdered:- " + maxSalaryGroupByKey.foreach(println))


    /**
      * *   === reduceByKey ===
      *
      */
    // reduceByKey((x,y)=> (x+y))
    // Eg-1 //    val maxSalaryReduceByKey = salaryWithEmployeeName.reduceByKey(_ + _).takeOrdered(2)(Ordering[Double].reverse.on(_._1))
    val maxSalaryReduceByKey = salaryWithEmployeeName.reduceByKey((x, y) => (x + y)).mapValues(_.toList).takeOrdered(2)(Ordering[Double].reverse.on(_._1))

    print("Max Salary using ReduceByKey.takeOrdered:- " + maxSalaryReduceByKey.foreach(println))

    /**
      *   === AggregateByKey  ===
      *
      * printing the values as array
      * */
    salaryWithEmployeeName.aggregateByKey(ListBuffer.empty[String])(
      (numList, num) => {
        numList += num; numList
      },
      (numList1, numList2) => {
        numList1.appendAll(numList2); numList1
      })
      .mapValues(_.toList)
      .collect().foreach(println)


    /**
      * When to use what ?
      *
      * groupByKey
      * -> groupByKey shuffles the data between partitions
      * -> reduceByKey would be a better option if the data set is large which does the same thing.
      *
      *
      * reduceByKey() - uses combiner internally by spark
      * -> Preferred for large datasets
      * -> First executes the lamda function within the partitions, first reducing, internal within partitions
      * -> then shuffles to reduce by finding out the right partitions of similar keys
      *
      * aggregateByKey
      * -> To be used when you want value in the form of list
      * -> Eg: in the above case, if 2 people have the 2nd highest salary then using aggregateByKey gets the listing
      */
  }
}
