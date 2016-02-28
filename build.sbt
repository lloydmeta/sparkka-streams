name := """sparkka"""

version := "1.0"

scalaVersion := "2.11.7"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "com.typesafe.akka" %% "akka-stream-experimental" % "2.0.3" % Provided,
  "org.apache.spark" %% "spark-mllib" % "1.6.0" % Provided
)

