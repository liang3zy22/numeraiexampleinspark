version := "1.0"
organization := "com.liang3z"
name := "numeraiexampleinspark"
scalaVersion := "2.13.8"
libraryDependencies += "org.apache.spark" %% """spark-sql""" % "3.2.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1"

scalacOptions += "-deprecation"
lazy val root = project in file(".")
