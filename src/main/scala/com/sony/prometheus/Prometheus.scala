package com.sony.prometheus

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.sony.prometheus.evaluation._
import com.sony.prometheus.interfaces._
import com.sony.prometheus.stages._
import com.sony.prometheus.utils.Utils.Colours._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.http4s.server.blaze._
import org.rogach.scallop._
import org.rogach.scallop.exceptions._

import scala.util.Properties.envOrNone

/** Main class, sets up and runs the pipeline
 */
object Prometheus {

  val DATA_PARTITIONS = 432
  val PORT = 8080

  /** Provides arugment parsing
   */
  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    version("Prometheus Model Trainer 0.0.1-SNAPSHOT")
    banner("""Usage: RelationModel [options] corpus-path entities-path temp-data-path --word2vecPath
           |Prometheus model trainer trains a relation extractor
           |Options:
           |""".stripMargin)
    val corpusPath = trailArg[String](
      descr = "path to the corpus to train on",
      validate = pathPrefixValidation)
    val entitiesPath = trailArg[String](
      descr = "path to a parquet file containing the entities/relations to train for",
      validate = pathPrefixValidation)
    val tempDataPath= trailArg[String](
      descr= "path to a directory that will contain intermediate results",
      validate = pathPrefixValidation)
    val word2vecPath = trailArg[String](
      descr = "path to a word2vec model in the C binary format",
      validate = pathPrefixValidation)
    val sampleSize = opt[Double](
      descr = "use this to sample a fraction of the corpus",
      validate = x => (x > 0 && x <= 1),
      default = Option(1.0))
    val demoServer = opt[Boolean](
      descr = "start an HTTP server to receive text to extract relations from")
    val evaluationFiles = opt[List[String]](descr = "path to evaluation files")
    val language = opt[String](
      required = true,
      default = Some("sv"),
      validate = l => l == "sv" || l == "en",
      descr = "the language to use for the pipeline (default to sv)")

    verify()

    override def onError(e: Throwable): Unit = e match {
      case ScallopException(message) =>
        println(message)
        printHelp
        sys.exit(1)
      case ex => super.onError(ex)
    }

    def pathPrefixValidation(path: String): Boolean = {
      path.split(":") match {
        case Array("hdfs", _) => true
        case Array("file", _) => true
        case _ => {
          System.err.println(s"""$path must be prefixed with either "hdfs:" or "file:"""")
          false
        }
      }
    }

  }

  def printEnvironment(conf: Conf): Unit = {
    val str =
      s"""corpusPath:\t${conf.corpusPath()}
         |entitiesPath:\t${conf.entitiesPath()}
         |tempDataPath:\t${conf.tempDataPath() + "/" + conf.language()}
         |word2vecPath:\t${conf.word2vecPath()}
         |sampleSize:\t${conf.sampleSize()}
         |evaluationFiles:\t${conf.evaluationFiles}
         |language:\t${conf.language()}
       """.stripMargin
    println(str)
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

    val tempDataPath = conf.tempDataPath() + "/" + conf.language()
    printEnvironment(conf)

    try {
      val corpusData = new CorpusData(conf.corpusPath())
      val relationsData = new RelationsData(conf.entitiesPath())
      val word2VecData = new Word2VecData(conf.word2vecPath())

      val trainingTask = new TrainingDataExtractorStage(
        tempDataPath + "/training_sentences",
        corpusData,
        relationsData)

      val posEncoderStage = new PosEncoderStage(
        tempDataPath + "/pos_encoder",
        corpusData)

      val featureExtractionTask = new FeatureExtractorStage(
        tempDataPath + "/features",
        trainingTask)

      val modelTrainingTask = new RelationModelStage(
        tempDataPath + "/models",
        featureExtractionTask,
        word2VecData,
        posEncoderStage)

      val path = modelTrainingTask.getData()
      log.info(s"Saved model to $path")

      // Evaluate
      conf.evaluationFiles.foreach(evaluate => {
        log.info("Performing evaluation")
        val f = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm.ss")
        val t = LocalDateTime.now()
        val predictor = Predictor(modelTrainingTask, posEncoderStage, word2VecData, relationsData)
        evaluate.foreach(evalFile => {
          log.info(s"Evaluating $evalFile")
          val evaluationData = new EvaluationData(evalFile)
          val evalSavePath = tempDataPath +
            s"/evaluation/${t.format(f)}-${evalFile.split("/").last.split(".json")(0)}"
          val evaluationTask = new EvaluatorStage(
            evalSavePath,
            evaluationData,
            conf.language(),
            predictor)
          val _ = evaluationTask.getData()
          log.info(s"Saved evaluation to $evalSavePath")
        })
      })

      // Serve HTTP API
      if (conf.demoServer()) {
        val predictor = Predictor(modelTrainingTask,  posEncoderStage, word2VecData, relationsData)
        try {
          val task = BlazeBuilder
            .bindHttp(PORT, "localhost")
            .mountService(REST.api(predictor), "/")
            .run
          println(s"${GREEN}REST interface ready to accept connections on $PORT ${RESET}")
          task.awaitShutdown()
        } catch  {
          case e: java.net.BindException => {
            println(s"${BOLD}${RED}Error:${RESET} ${e.getMessage}")
            sc.stop()
            sys.exit(1)
          }
        }
      }
    } finally {
      sc.stop()
    }
  }
}

