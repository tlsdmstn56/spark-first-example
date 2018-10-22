name := "GreenButton ETL"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "commons-collections" % "commons-collections" % "3.2.2"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1" % "provided"