name := """fact-extractor"""

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.6"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value
// Uncomment to use Akka
//libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.11"

