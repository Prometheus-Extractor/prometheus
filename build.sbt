name := """prometheus-relation-model"""

packAutoSettings

version := "0.8.0"

organization := "sonymobile"

scalaVersion := "2.10.6"
ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }
dependencyOverrides += "org.scala-lang" % "scala-compiler" % scalaVersion.value

publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

resolvers += Resolver.mavenLocal
resolvers += "Akka repository" at "http://repo.akka.io/releases"
resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers += "JBoss" at "https://repository.jboss.org/"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.6"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % Test
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "1.6.0_0.6.0" % Test
libraryDependencies += "org.rogach" %% "scallop" % "2.1.0"
libraryDependencies += "se.lth.cs.nlp" % "docforia" % "1.0-SNAPSHOT"
libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.3.10"

//libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "0.8.0"
libraryDependencies += "org.deeplearning4j" % "dl4j-spark-nlp_2.10" % "0.8.0_spark_1"
libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "0.8.0"
// libraryDependencies += "org.nd4j" % "nd4j-cuda-8.0" % "0.8.0"


val http4sVersion = "0.15.6"
libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-blaze-client" % http4sVersion
)

lazy val spark_core_version = "1.6.0-cdh5.10.0"
lazy val spark_sql_version = "1.6.0-cdh5.10.0"
lazy val spark_mlib_version = "1.6.0-cdh5.10.0"

lazy val sparks = Seq(
  "org.xerial.snappy" % "snappy-java" %"1.1.2.1",
  "org.apache.spark" % "spark-core_2.10" % spark_core_version,
  "org.apache.spark" % "spark-mllib_2.10" % spark_mlib_version,
  "org.apache.spark" % "spark-sql_2.10" % spark_sql_version
)

if (sys.props.getOrElse("mode", default = "local") == "cluster") {
  println("Running for cluster!")
  libraryDependencies ++= sparks.map(_ % "provided")
} else {
  libraryDependencies ++= sparks
}


// ScalaDoc site generation
enablePlugins(SiteScaladocPlugin)
enablePlugins(GhpagesPlugin)
git.remoteRepo := "git@github.com:ErikGartner/prometheus-relation-model.git"
siteSubdirName in SiteScaladoc := "api/" + version.value
// excludeFilter in ghpagesCleanSite :=
  // new FileFilter{
    // def accept(f: File) = (ghpagesRepository.value / "api" ** "*").absString == f.getCanonicalPath
  // }
enablePlugins(PreprocessPlugin)
sourceDirectory in Preprocess := sourceDirectory.value / "site-preprocess"
preprocessVars in Preprocess := Map("VERSION" -> version.value)

// enjoy ScalaTest's built-in event buffering algorithm
logBuffered in Test := false

parallelExecution in Test := false
