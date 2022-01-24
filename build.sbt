ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "SparkProject_2"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.2.0"
libraryDependencies += "org.apache.avro"  %  "avro"  %  "1.7.7"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

