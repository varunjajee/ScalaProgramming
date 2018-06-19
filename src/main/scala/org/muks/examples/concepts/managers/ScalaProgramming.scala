package org.muks.examples.concepts.managers

import org.muks.examples.concepts.businessobjects.Student

object ScalaProgramming extends App {
  val ages = Seq(42, 75, 29, 64)
  println(s"The oldest person is ${ages.max}")
  println("Runtime compilition....woh")

  println(args.mkString(", "))

  var student = new Student("Muks", 35)
  println(student.toString)
}
