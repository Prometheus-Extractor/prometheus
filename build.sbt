name := """fact-extractor"""

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.10.6"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value
resolvers += Resolver.mavenLocal
