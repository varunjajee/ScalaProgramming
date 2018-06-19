package org.muks.examples.concepts.variables

object VariablesAndDataTypes {

  def main(args: Array[String]): Unit = {
    println("# Awesome")


  // mutable variable
    var data : Int = 100;
    data = 1234;
    data = 123
    println("$ Mutable variable: ", data)


    val immutableVariable = "Muks"
    //  immutableVariable = "Mukthar"


    var name : String = "Mukthar"
    name = "Mukthar Ahmed"
    println("Name", name)
  }

}
