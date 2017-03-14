package com.sony.prometheus

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
    // TODO: use scalop arg
    val evaluationData = new EvaluationData("./evaluationdata.txt")
    val docs = EvaluationDataReader.getAnnotatedDocs(evaluationData.getData())
    val predictorTask = new PredictorStage(
      conf.tempDataPath() + "/predictions",
      modelTrainingTask,
      featureTransformerTask,
      relationsData,
      docs)
    val evaluationTask = new EvaluatorStage(
      conf.tempDataPath() + "/evaluation", // TODO: timestamp
      predictorTask,
      evaluationData)

    val evaluationPath = evaluationTask.getData()
    log.info(s"Saved evaluation to $path")

    // Server HTTP API
    val predictor = Predictor(modelTrainingTask, featureTransformerTask, relationsData)
    var task: Server = null
    val blaze = BlazeBuilder.bindHttp(8080, "localhost")
    task = blaze.mountService(REST.api(task, predictor), "/api").run
    task.awaitShutdown()
    println("Shutting down REST API")

    sc.stop()

  }
}

