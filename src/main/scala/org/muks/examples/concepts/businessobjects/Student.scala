package org.muks.examples.concepts.businessobjects


/**
  * Student class object
  * @param name - name string
  * @param age - age int
  */
class Student(name: String, age: Int) {
  var branch: String = "undef"

  def this(name: String, age: Int, branch: String) {
    this(name, age)
    this.branch = branch
  }

  override def toString: String = "[Name: " + this.name + ", Age: " + this.age + ", Branch: " + this.branch + "]"
}

/**
  * Demo main object
  */
object MainObject {
  def main(args: Array[String]): Unit = {
    var student = new Student("Muks", 36)
    println(student.toString)
  }
}