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
    version("Prometheus Model Trainer")
    banner("""Usage: RelationModel [options] stage corpus-path entities-path temp-data-path word2vecPath
           |Prometheus model trainer trains a relation extractor
           |Options:
           |""".stripMargin)
    val stage = trailArg[String](
      descr = """how far to run the program, [preprocess|train|full]
        |train implies preprocess
        |full implies train
        |""".stripMargin,
      validate = s => s == "preprocess" || s == "train" || s == "full",
      default = Some("full"))

    val corpusPath = trailArg[String](
      descr = "path to the corpus to train on",
      validate = pathPrefixValidation)
    val relationConfig = trailArg[String](
      descr = "path to a TSV listing the desired relations to train for",
      validate = pathPrefixValidation)
    val wikiData = trailArg[String](
      descr = "path to the wikidata dump in parquet",
      validate = pathPrefixValidation)
    val tempDataPath = trailArg[String](
      descr= "path to a directory that will contain intermediate results",
      validate = pathPrefixValidation)
    val word2vecPath = trailArg[String](
      descr = "path to a word2vec model in the C binary format",
      validate = pathPrefixValidation)
    val sampleSize = opt[Double](
      descr = "use this to sample a fraction of the corpus",
      validate = x => (x > 0 && x <= 1),
      default = Option(1.0))
    val probabilityCutoff = opt[Double](
      descr = "use this to sample a fraction of the corpus",
      validate = x => (x >= 0 && x <= 1),
      default = Option(RelationModel.THRESHOLD))
    val demoServer = opt[Boolean](
      descr = "start an HTTP server to receive text to extract relations from")
    val evaluationFiles = opt[List[String]](descr = "path to evaluation files")
    val epochs = opt[Int](
      descr = "number of epochs for neural network",
      validate = x => (x > 0),
      default = Option(5))
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

    private def pathPrefixValidation(path: String): Boolean = {
      path.split(":") match {
        case Array("hdfs", _) => true
        case Array("file", _) => true
        case Array("s3", _) => true
        case _ => {
          System.err.println(s"""$path must be prefixed with either "hdfs:" or "file: or s3:"""")
          false
        }
      }
    }

  }

  def printEnvironment(conf: Conf): Unit = {
    val str =
      s"""corpusPath:\t${conf.corpusPath()}
         |relationConfigPath:\t${conf.relationConfig()}
         |wikidataPath:\t${conf.wikiData()}
         |tempDataPath:\t${conf.tempDataPath() + "/" + conf.language()}
         |word2vecPath:\t${conf.word2vecPath()}
         |sampleSize:\t${conf.sampleSize()}
         |evaluationFiles:\t${conf.evaluationFiles}
         |language:\t${conf.language()}
         |epochs:\t${conf.epochs()}
       """.stripMargin
    println(str)
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val log = LogManager.getLogger(Prometheus.getClass)
    val appName = s"Prometheus Relation Model ${conf.language()} ${conf.stage()}"
    val sparkConf = new SparkConf().setAppName(appName)
    envOrNone("SPARK_MASTER").foreach(m => sparkConf.setMaster(m))

    implicit val sc = new SparkContext(sparkConf)
    implicit val sqlContext = new SQLContext(sc)

    val tempDataPath = conf.tempDataPath() + "/" + conf.language()
    printEnvironment(conf)

    try {
      val corpusData = new CorpusData(conf.corpusPath(), conf.sampleSize())
      val word2VecData = new Word2VecData(conf.word2vecPath())
      val configData = new RelationConfigData(conf.relationConfig())
      val wikidata = new WikidataData(conf.wikiData())

      // Extract entity pairs that participate in the given relations
      val entityPairs = new EntityPairExtractorStage(
        tempDataPath + "/relation_entity_pairs",
        configData,
        wikidata
      )

      // Extract training sentences
      val trainingExtactorTask = new TrainingDataExtractorStage(
        tempDataPath + "/training_sentences",
        corpusData,
        entityPairs)

      val posEncoderStage = new PosEncoderStage(
        tempDataPath + "/pos_encoder",
        corpusData)

      val neTypeEncoderStage = new NeTypeEncoderStage(
        tempDataPath + "/netype_encoder",
        corpusData)

      val depEncoder = new DependencyEncoderStage(
        tempDataPath + "/dep_encoder",
        corpusData)

      // Extract features from the training data (Strings)
      val featureExtractionTask = new FeatureExtractorStage(
        tempDataPath + "/features",
        trainingExtactorTask,
        configData)

      // Transform features from Strings into vectors of numbers
      val featureTransformerStage = new FeatureTransformerStage(
        tempDataPath + "/vector_features",
        word2VecData,
        posEncoderStage,
        neTypeEncoderStage,
        depEncoder,
        featureExtractionTask)

      if (conf.stage() == "preprocess") {
        val featuresPath = featureTransformerStage.getData()
        log.info(s"Entity pairs saved to ${featuresPath}")
      } else {

        // Train models
        val classificationModelStage = new ClassificationModelStage(
          tempDataPath + "/classification_model",
          featureTransformerStage,
          conf.epochs()
        )

        val filterModelStage = new FilterModelStage(
          tempDataPath + "/filter_model",
          featureTransformerStage
        )

        val relationModel = RelationModel(filterModelStage, classificationModelStage, conf.probabilityCutoff())

        if (conf.stage() == "train") {
          filterModelStage.getData()
          log.info(s"Saved model to ${classificationModelStage.getData()} and ${filterModelStage.getData()}")
        } else {
          // Evaluate
          conf.evaluationFiles.foreach(evalFiles => {
            log.info("Performing evaluation")
            val predictor = Predictor(relationModel, posEncoderStage, word2VecData, neTypeEncoderStage,
              depEncoder, configData)
            performEvaluation(evalFiles, predictor, conf.language(), log, tempDataPath)
          })

          // Serve HTTP API
          if (conf.demoServer()) {
            val predictor = Predictor(relationModel,  posEncoderStage, word2VecData, neTypeEncoderStage, depEncoder, configData)
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

          // Start generating data
          val predictorStage = new PredictorStage(
            tempDataPath + "/extractions",
            corpusData,
            relationModel,
            posEncoderStage,
            word2VecData,
            neTypeEncoderStage,
            depEncoder,
            configData)

          // Force running
          predictorStage.run()

        }

      }
      log.info("Successfully completed all requested stages!")
    } finally {
      sc.stop()
    }
  }

  private def performEvaluation(
    evalFiles: List[String],
    predictor: Predictor,
    lang: String,
    log: Logger,
    tempDataPath: String)
    (implicit sqlContext: SQLContext, sc: SparkContext): Unit = {

    val N_MOST_PROBABLE = 100

    val f = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH.mm.ss")
    val t = LocalDateTime.now()

    evalFiles.foreach(evalFile => {
      log.info(s"Evaluating $evalFile")
      val evaluationData = new EvaluationData(evalFile)
      val evalSavePath = tempDataPath +
        s"/evaluation/${t.format(f)}-${evalFile.split("/").last.split(".json")(0)}"
      val evaluationTask = new EvaluatorStage(
        evalSavePath,
        evaluationData,
        lang,
        predictor,
        N_MOST_PROBABLE)
      val _ = evaluationTask.getData()
      log.info(s"Saved evaluation to $evalSavePath")
    })
  }
}

