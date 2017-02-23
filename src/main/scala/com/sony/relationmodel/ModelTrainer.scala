package com.sony.relationmodel

import org.apache.log4j.LogManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._
import org.rogach.scallop.exceptions._

import scala.util.Properties.envOrNone

object ModelTrainer {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    version("Prometheus Model Trainer 0.0.1-SNAPSHOT")
    banner("""Usage: ModelTrainer [--sample-size=0.f] corpus-path relations-path temp-data-path
           |Prometheus model trainer trains a relation extractor
           |Options:
           |""".stripMargin)
    val corpusPath = trailArg[String](descr = "path to the corpus to train on")
    val relationsPath = trailArg[String](descr = "path to a parquet file with the relations")
    val tempDataPath= trailArg[String](descr= "path to a folder that will contain intermediate results")
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
    val corpusData = new CorpusData(conf.corpusPath())
    val trainingTask = new TrainingDataExtractorStage(
      conf.tempDataPath() + "/training_sentences",
      corpusData = corpusData,
      relationsData = new RelationsData(conf.relationsPath()))
    val featureTransformerTask = new FeatureTransformerStage(conf.tempDataPath() + "/feature_model", corpusData)
    val featureExtractionTask = new FeatureExtractorStage(conf.tempDataPath() + "/features", featureTransformerTask, trainingTask)

    featureExtractionTask.run()

    sc.stop()
  }

}

