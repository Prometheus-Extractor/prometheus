package com.sony.prometheus.evaluation

import java.io.BufferedOutputStream
import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.JavaConverters._

import com.sony.prometheus.utils.Utils.pathExists
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import com.sony.prometheus.stages.{Predictor, _}
import com.sony.prometheus.utils.Utils
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import com.sony.prometheus.annotaters.VildeAnnotater
import org.apache.hadoop.fs.{FileSystem, Path}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Token
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
  lang: String,
  predictor: Predictor)
  (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val evalDataPoints: RDD[EvaluationDataPoint] = EvaluationDataReader.load(evaluationData.getData())
      .filter(dP => dP.wd_sub != "false" && dP.wd_obj != "false")
    val annotatedEvidence = Evaluator.annotateTestData(evalDataPoints, path, lang)
    val evaluation = Evaluator.evaluate(evalDataPoints, annotatedEvidence, predictor, Some(path + "_debug.tsv"))
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
  def annotateTestData(evalDataPoints: RDD[EvaluationDataPoint], path: String, lang: String)
                      (implicit sqlContext: SQLContext, sc: SparkContext): RDD[Document] = {
    import sqlContext.implicits._

    val file = path.split("-").last
    val l = path.split("/").length
    val cachePath = path.split("/").slice(0, l - 1).mkString("/") + "/cache/" + file + ".cache"
    log.info(s"Caching $file to $cachePath")

    if (Utils.pathExists(cachePath)) {
      log.info(s"Using cached Vilde-annotated $file")
      val df = sqlContext.read.parquet(cachePath)
      df.map(row => {
        MemoryDocumentIO.getInstance().fromBytes(row.getAs(0): Array[Byte]): Document
      })
    } else {
      log.info(s"Did not find cached $file, annotating with Vilde...")
      val annotatedEvidence =
        evalDataPoints
          // treat multiple snippets as one string of multiple paragraphs
          .map(dP => dP.evidences.map(_.snippet).mkString("\n"))
          .map(e => VildeAnnotater.annotate(e, lang = lang, conf = "herd"))
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
    * @return                 an [[EvaluationResult]]
   */
  def evaluate(evalDataPoints: RDD[EvaluationDataPoint], annotatedEvidence: RDD[Document], predictor: Predictor, debugOutFile: Option[String] = None)
    (implicit sqlContext: SQLContext, sc: SparkContext): EvaluationResult = {

    val nbrEvalDataPoints = evalDataPoints.count()

    val herdEnts = evalDataPoints.zip(annotatedEvidence).filter{case (dP, evidence) => {
      val ents = evidence.nodes(classOf[NamedEntityDisambiguation]).asScala.toSeq.map(_.getIdentifier.split(":").last)
      ents.contains(dP.wd_obj) && ents.contains(dP.wd_sub)
    }}.count()

    val herdRecall = herdEnts / nbrEvalDataPoints
    log.info(s"HERD recall is $herdRecall")

    val nbrTrueDataPoints: Double = evalDataPoints
      .filter(dP => dP.judgments.filter(_.judgment == "yes").length > dP.judgments.length / 2.0)
      .count()
    log.info(s"There are ${nbrTrueDataPoints.toInt} positive examples in the evaluation data")
    log.info(s"There are ${nbrEvalDataPoints - nbrTrueDataPoints} negative examples in the evaluation data")

    val predictedRelations = predictor.extractRelations(annotatedEvidence)
    predictedRelations.cache()

    val nbrPredictedRelations: Double = predictedRelations
      .map(rels => rels.filter(!_.predictedPredicate.contains(predictor.UNKNOWN_CLASS)))
      .flatMap(rels => rels)
        .map(rel => {
          rel
        })
      .count()
    log.info(s"Extracted ${nbrPredictedRelations.toInt} relations from evaluation data")

    // Evaluate positive examples
    val nbrTruePositives = evalDataPoints.zip(predictedRelations)
      .filter{ case (dP, _) =>
        dP.judgments.filter(_.judgment == "yes").length > dP.judgments.length / 2.0} // majority said yes
      .filter{case (dP, rels) =>
      rels.exists(rel => {
        log.info(s"Our prediction: $rel <==> $dP")
        dP.wd_obj == rel.obj && dP.wd_sub == rel.subject && dP.wd_pred == rel.predictedPredicate
        })
      }
      .count()

    // Save debug information to TSV if debugOutFile supplied
    debugOutFile.foreach(f => {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val output = fs.create(new Path(f))
      val os = new BufferedOutputStream(output)
      log.info(s"Saving debug information to $f...")
      val data = evalDataPoints.zip(predictedRelations)
        .filter{ case (dP, _) =>
          dP.judgments.filter(_.judgment == "yes").length > dP.judgments.length / 2.0}
        .flatMap{case (dP, rels) => rels.map(rel => s"$rel\t$dP")
      }.collect().mkString("\n")

      os.write("predicted relation\tevaluation data point".getBytes("UTF-8"))
      os.write(data.getBytes("UTF-8"))
      os.close()
    })

    log.info(s"truePositives: ${nbrTruePositives}")

    val recall: Double = nbrTruePositives / nbrTrueDataPoints
    val precision: Double = nbrTruePositives / nbrPredictedRelations

    log.info(s"~precision is $precision")
    log.info(s"recall is $recall")

    val f1 = computeF1(recall, precision)
    val evaluation: EvaluationResult = EvaluationResult(
      evalDataPoints.first().wd_pred,
      nbrTrueDataPoints.toInt,
      nbrTruePositives.toInt,
      recall,
      precision,
      f1,
      herdRecall)
    log.info(s"EvaluationResult: $evaluation")

    evaluation
  }

  private def computeF1(recall: Double, precision: Double): Double =
    2 * (precision * recall) / (precision + recall)

  def save(data: EvaluationResult, path: String)(implicit sc: SparkContext): Unit = {
    // Hadoop Config is accessible from SparkContext
    val fs = FileSystem.get(sc.hadoopConfiguration)

    // Output file can be created from file system.
    val output = fs.create(new Path(path + ".tsv"))

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
    f1: Double,
    herdRecall: Double) {

    override def toString: String = {
      s"""relation\tnbr data points\ttrue positives\trecall\tprecision\tf1\tHERD recall
         |$relation\t$nbrDataPoints\t$truePositives\t$recall\t$precision\t$f1\t$herdRecall
      """.stripMargin
    }
  }
}

