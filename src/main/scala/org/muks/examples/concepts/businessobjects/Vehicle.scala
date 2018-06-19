package org.muks.examples.concepts.businessobjects


/**
  * Default constructure is at the class signature level
  */


class Vehicle(name: String) {
  //var name: String = ""
  var age: Int = 0

  def this(nm: String, age: Int){
    //this(nm, age)
    this(nm)
    this.age = age
  }

  override def toString: String = "[Name=" + this.name + ", Age=" + this.age + "]"

}

object MainObj {
  def main(args: Array[String]): Unit = {
    var vehicle = new Vehicle("Vento", 4)
    println("Vehicle: " + vehicle.toString)
  }
}