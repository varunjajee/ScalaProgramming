//import sbt.Keys.libraryDependencies
//
//name := "ScalaProgramming"
//version := "0.1"
//scalaVersion := "2.11.0"
//crossScalaVersions := Seq("2.10.2", "2.10.3", "2.11.8")
//
//libraryDependencies ++= Seq(
//  "org.scala-lang.modules" % "scala-parser-combinators" % "1.1.0",
//  "org.scala-lang" % "scala-library" % "2.11.1",
//  "org.apache.spark" % "spark-core" % "2.1.1",
//  "org.apache.spark" % "spark-sql_2.11"
//
//)

import sbt.Keys.libraryDependencies

name := "bitcoin-trending"
version := "1.0"
scalaVersion := "2.11.0"
crossScalaVersions := Seq("2.10.2", "2.10.3", "2.11.8")

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.11.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "org.apache.spark" %% "spark-core" % "2.1.1"

)