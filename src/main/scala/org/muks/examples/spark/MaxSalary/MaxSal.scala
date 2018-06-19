package org.muks.examples.spark.MaxSalary

import org.apache.spark.{SparkConf, SparkContext}

object MaxSal {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Emp Dept Assignment").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val resourcesPath = getClass.getResource("/emp.csv") // Load the source data file into RDD
    println(resourcesPath.getPath)

    val empDataRDD = sc.textFile(resourcesPath.getPath)
    //println(empDataRDD.foreach(println))

    // Find first record of the file
    val headerLine = empDataRDD.first()
    println("Header line:- " + headerLine)

    // Remove header from RDD
    val empDataRdd = empDataRDD.filter(someLine => !someLine.equals(headerLine))
    println(empDataRdd.foreach(println))


    // Get no. of partition of an RDD
    println("No. of partition = " + empDataRdd.partitions.size)


    /**
      * Finding max using map function and map.max()
      */
    val empSalaryListMap = empDataRdd.map { x => x.split(',') }.map { x => (x(5).toDouble) }
    empSalaryListMap.collect().foreach(println)

    println("Highest salary = " + empSalaryListMap.max())


    /**
      * Min and Max using sort and take function
      */
    val salarySortedDescending = empSalaryListMap.distinct.sortBy(x => x.toDouble, false, 1)
    salarySortedDescending.collect().foreach(println)
    println("Max Salary using sortBy:- ",  salarySortedDescending.take(1).foreach(println))


    // Find minimum salary
    val salarySortedAsc = empSalaryListMap.distinct.sortBy(x => x.toDouble, true, 1)
    println("Min Salary using sortBy:- " + salarySortedAsc.take(1).foreach(println))


    /*
      * Using zipWithIndex() and filter by the index value where index._2 (double) == 0 | 1 | 2 or 3 and so on.
      */
    val second_highest_salary = salarySortedDescending.zipWithIndex().filter(index => index._2 == 3) //.map(_._1)
    println("Second highest Salary using sortBy:- " + second_highest_salary.foreach(println))


    // Find second min salary
    val second_min_salary = salarySortedAsc.zipWithIndex().filter(index => index._2 == 1).map(_._1)
    print("Second min Salary using zipWithIndex():- " + second_min_salary.foreach(println))


    /*
      * USing groupBy and takeOrdered( - over reverse ordering - )
      */
    val salaryWithEmployeeName = empDataRdd.map { x => x.split(',') }.map { x => (x(5).toDouble, x(1)) }
    val maxSalaryEmployee = salaryWithEmployeeName.groupByKey.takeOrdered(2)(Ordering[Double].reverse.on(_._1))
    print("Max Salary using groupByKey.takeOrdered:- " + maxSalaryEmployee.foreach(println))

  }


}
