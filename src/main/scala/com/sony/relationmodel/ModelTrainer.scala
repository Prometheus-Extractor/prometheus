package com.sony.relationmodel

import scala.collection.JavaConverters._
import java.io.IOError
import java.util.regex.Pattern

import org.apache.log4j.LogManager
import org.apache.spark.ml.feature.{HashingTF, OneHotEncoder, StringIndexer, Word2Vec}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.{NamedEntity, Sentence, Token}
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.QueryCollectors
import org.rogach.scallop._
import org.rogach.scallop.exceptions._
import scala.util.Properties.envOrNone
import java.nio.file.{Paths, Files}

object ModelTrainer {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    version("Prometheus Model Trainer 0.0.1-SNAPSHOT")
    banner("""Usage: ModelTrainer [--force] [--sample-size=0.f] corpus-path relations-path temp-data-path
           |Prometheus model trainer trains a relation extractor
           |Options:
           |""".stripMargin)
    val corpusPath = trailArg[String](descr = "path to the corpus to train on")
    val relationsPath = trailArg[String](descr = "path to a parquet file with the relations")
    val tempDataPath= trailArg[String](descr= "path to a folder that will contain intermediate results")
    val force = opt[Boolean](descr = "set this to invalidate cached intermediate results")
    val sampleSize = opt[Double](descr = "use this sample a fraction of the corpus", validate = x => (x > 0 && x <= 1), default = Option(1.0))

    verify()

    override def onError(e: Throwable): Unit = e match {
      case ScallopException(message) =>
        println(message)
        printHelp
        sys.exit(1)
      case ex => super.onError(ex)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val log = LogManager.getRootLogger
    val sparkConf = new SparkConf().setAppName("Prometheus Relation Model")
    envOrNone("SPARK_MASTER").foreach(m => sparkConf.setMaster(m))

    implicit val sc = new SparkContext(sparkConf)
    implicit val sqlContext = new SQLContext(sc)

    // want to do get relations, docs, trainingData

    val trainingTask = new TrainingDataExtractorStage(
      conf.tempDataPath() + "/training_sentences",
      corpusData = new Data{def getData(force: Boolean) = conf.corpusPath()},
      relationsData = new Data{def getData(force: Boolean) = conf.relationsPath()})

    trainingTask.run()

    sc.stop()
  }

}

