package com.sony.prometheus

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._
import org.rogach.scallop.exceptions._
import pipeline._

import scalaz.concurrent.{Task => HttpTask}
import pipeline._
import interfaces._
import org.http4s.server.{Server, ServerApp}
import org.http4s.server.blaze._

import scala.util.Properties.envOrNone
import scala.io.Source
import annotaters.VildeAnnotater
import evaluation._

/** Main class, sets up and runs the pipeline
 */
object Prometheus {

  val DATA_PARTITIONS = 432

  /** Provides arugment parsing
   */
  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    version("Prometheus Model Trainer 0.0.1-SNAPSHOT")
    banner("""Usage: RelationModel [--sample-size=0.f] corpus-path relations-path temp-data-path
           |Prometheus model trainer trains a relation extractor
           |Options:
           |""".stripMargin)
    val corpusPath = trailArg[String](descr = "path to the corpus to train on")
    val relationsPath = trailArg[String](descr = "path to a parquet file with the relations")
    val tempDataPath= trailArg[String](descr= "path to a folder that will contain intermediate results")
    val sampleSize = opt[Double](
      descr = "use this sample a fraction of the corpus",
      validate = x => (x > 0 && x <= 1),
      default = Option(1.0))
    val demoServer = opt[Boolean](
      descr = "start an HTTP server to receive text to extract relations from")
    val evaluationFile = opt[String](descr = "path to file to evaluate")

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

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val log = LogManager.getLogger(Prometheus.getClass)
    val sparkConf = new SparkConf().setAppName("Prometheus Relation Model")
    envOrNone("SPARK_MASTER").foreach(m => sparkConf.setMaster(m))

    implicit val sc = new SparkContext(sparkConf)
    implicit val sqlContext = new SQLContext(sc)

    val corpusData = new CorpusData(conf.corpusPath())
    val relationsData = new RelationsData(conf.relationsPath())
    val trainingTask = new TrainingDataExtractorStage(
      conf.tempDataPath() + "/training_sentences",
      corpusData,
      relationsData)
    val featureTransformerTask = new FeatureTransformerStage(
      conf.tempDataPath() + "/feature_model",
      corpusData)
    val featureExtractionTask = new FeatureExtractorStage(
      conf.tempDataPath() + "/features",
      featureTransformerTask,
      trainingTask)
    val modelTrainingTask = new RelationModelStage(
      conf.tempDataPath() + "/models",
      featureExtractionTask,
      featureTransformerTask,
      relationsData)

    val path = modelTrainingTask.getData()
    log.info(s"Saved model to $path")

    // Evaluate
    val f = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm.ss")
    val t = LocalDateTime.now()
    conf.evaluationFile.toOption.foreach(evalFile => {
      val evaluationData = new EvaluationData(evalFile)
      val predictor = Predictor(modelTrainingTask, featureTransformerTask, relationsData)
      val evaluationTask = new EvaluatorStage(
        conf.tempDataPath() + s"/evaluation/${t.format(f).toString}",
        evaluationData,
        predictor)
      val evaluationPath = evaluationTask.getData()
      log.info(s"Saved evaluation to $path")
    })

    // Serve HTTP API
    if (conf.demoServer()) {
      val predictor = Predictor(modelTrainingTask, featureTransformerTask, relationsData)
      var task: Server = null
      val blaze = BlazeBuilder.bindHttp(8080, "localhost")
      task = blaze.mountService(REST.api(task, predictor), "/api").run
      task.awaitShutdown()
      println("Shutting down REST API")
    }

    sc.stop()

  }
}

