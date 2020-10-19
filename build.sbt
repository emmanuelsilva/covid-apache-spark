name := "covid-spark-scala"

version := "0.1"

scalaVersion := "2.12.1"
val sparkVersion = "3.0.1"

//spark dependencies
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

//testing libraries
libraryDependencies += "org.scalatest"    %% "scalatest" % "3.0.8" % Test
