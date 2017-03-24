package com.sony.prometheus.evaluation

import java.io.BufferedOutputStream
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import com.sony.prometheus.pipeline._
import com.sony.prometheus.Predictor
import com.sony.prometheus.utils.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import com.sony.prometheus.annotaters.VildeAnnotater
import org.apache.hadoop.fs.{FileSystem, Path}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.memstore.MemoryDocumentIO


/** Pipeline stage to run evaluation
 *
 * @param path            path to save evaluation results
 * @param evaluationData  the Data to evaluate, should point to path with
 * [[EvaluationDataPoint]]:s
 */
class EvaluatorStage(
  path: String,
  evaluationData: Data,
  predictor: Predictor)
  (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val evalDataPoints: RDD[EvaluationDataPoint] = EvaluationDataReader.load(evaluationData.getData())
      .filter(dP => dP.wd_sub != "false" && dP.wd_obj != "false")
    val annotatedEvidence = Evaluator.annotateTestData(evalDataPoints, path)
    val evaluation = Evaluator.evaluate(evalDataPoints, annotatedEvidence, predictor)
    Evaluator.save(evaluation, path)
  }
}

/** Performs evaluation of [[EvaluationDataPoint]]:s
 */
object Evaluator {


  /** Annotate the snippets data with VildeAnnotater, use cache if possible
    *
    * @param evalDataPoints   [[EvaluationDataPoint]]:s to annotate
    * @param path             path to the cache file
    * @return                 RDD of annotateted Documents
    */
  def annotateTestData(evalDataPoints: RDD[EvaluationDataPoint], path: String)
                      (implicit sqlContext: SQLContext, sc: SparkContext): RDD[Document] = {
    import sqlContext.implicits._

    val relation = evalDataPoints.first.wd_pred
    val l = path.split("/").length
    val cachePath = path.split("/").slice(0, l - 1).mkString("/") + "/cache/" + relation
    log.info(s"Caching to $cachePath")

    if (Utils.pathExists(cachePath)) {
      log.info("Using cached Vilde-annotated test data")
      val df = sqlContext.read.parquet(cachePath)
      df.map(row => {
        MemoryDocumentIO.getInstance().fromBytes(row.getAs(0): Array[Byte]): Document
      })
    } else {
      log.info("Did not find cached test data, annotating with Vilde...")
      val annotatedEvidence =
        evalDataPoints
          // treat multiple snippets as one string of multiple paragraphs
          .map(dP => dP.evidences.map(_.snippet).mkString("\n"))
          .map(e => VildeAnnotater.annotate(e, lang = "en", conf = "herd"))
      annotatedEvidence
        .map(doc => Tuple1(doc.toBytes))
        .toDF("doc")
        .write.parquet(cachePath)
      annotatedEvidence
    }
  }

  val log = LogManager.getLogger(Evaluator.getClass)

  /** Returns an [[EvaluationResult]]
    * @param evalDataPoints   RDD of [[EvaluationDataPoint]]
    * @return                 an [[EvaluationResult]] (relation, recall, precision, f1)
   */
  def evaluate(evalDataPoints: RDD[EvaluationDataPoint], annotatedEvidence: RDD[Document], predictor: Predictor)
    (implicit sqlContext: SQLContext, sc: SparkContext): EvaluationResult = {

    val nbrDataPoints: Double = evalDataPoints
      .filter(dP => dP.judgments.filter(_.judgment == "yes").length > dP.judgments.length / 2.0)
      .count()
    log.info(s"There are ${nbrDataPoints.toInt} true-judged EvaluationDataPoints")

    val predictedRelations = predictor.extractRelations(annotatedEvidence)

    predictedRelations.cache()

    val nbrPredictedRelations: Double = predictedRelations
      .map(rels => rels.filter(!_.predictedPredicate.contains(predictor.UNKNOWN_CLASS)))
      .flatMap(rels => rels)
        .map(rel => {
          rel
        })
      .count()
    log.info(s"Extracted ${nbrPredictedRelations.toInt} relations")

    // Evaluate positive examples
    val truePositives = evalDataPoints.zip(predictedRelations)
      .filter{ case (dP, _) =>
        dP.judgments.filter(_.judgment == "yes").length > dP.judgments.length / 2.0} // majority said yes
      .filter{case (dP, rels) =>
        rels.exists(rel => {
          log.info(s"Our prediction: $rel ==> $dP")
          dP.wd_obj == rel.obj && dP.wd_sub == rel.subject && dP.wd_pred == rel.predictedPredicate
        })
      }
      .count()

    log.info(s"truePositives: ${truePositives}")

    val recall: Double = truePositives / nbrDataPoints
    val precision: Double = truePositives / nbrPredictedRelations

    log.info(s"precision is $precision")
    log.info(s"recall is $recall")

    val f1 = computeF1(recall, precision)
    val evaluation: EvaluationResult = EvaluationResult(
      evalDataPoints.first().wd_pred,
      nbrDataPoints.toInt,
      truePositives.toInt,
      recall,
      precision,
      f1)
    log.info(s"EvaluationResult: $evaluation")

    evaluation
  }

  private def computeF1(recall: Double, precision: Double): Double =
    2 * (precision * recall) / (precision + recall)

  def save(data: EvaluationResult, path: String)(implicit sc: SparkContext): Unit = {
    // Hadoop Config is accessible from SparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // Output file can be created from file system.
    val output = fs.create(new Path(path))

    // But BufferedOutputStream must be used to output an actual text file.
    val os = new BufferedOutputStream(output)

    os.write(data.toString.getBytes("UTF-8"))

    os.close()

  }

  case class EvaluationResult(
    relation: String,
    nbrDataPoints: Int,
    truePositives: Int,
    recall: Double,
    precision: Double,
    f1: Double) {

    override def toString: String = {
      s"""relation\tnbr data points\ttrue positives\trecall\tprecision\tf1
         |$relation\t$nbrDataPoints\t$truePositives\t$recall\t$precision\t$f1
      """.stripMargin
    }
  }
}

