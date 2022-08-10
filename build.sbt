version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "airbnb_sparkML"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.2.1" % "provided"
