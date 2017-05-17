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
import se.lth.cs.docforia.memstore.{MemoryDocument, MemoryDocumentIO}

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
  predictor: Predictor,
  nBest: Int)
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
    val evaluation = Evaluator.evaluate(evalDataPoints, annotatedEvidence, predictor, Some(path + "_debug.tsv"), Some(nBest))

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
          .map{e =>
            val docEither = VildeAnnotater.annotate(e, lang = lang, conf = "herd")
            docEither match {
              case Right(doc) => doc
              case Left(msg) => {
                log.error(msg)
                new MemoryDocument()
              }
            }
          }
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
  def evaluate(evalDataPoints: RDD[EvaluationDataPoint], annotatedEvidence: RDD[Document], predictor: Predictor,
               debugOutFile: Option[String], nMostProbable: Option[Int])
    (implicit sqlContext: SQLContext, sc: SparkContext): EvaluationResult = {

    evalDataPoints.cache()
    annotatedEvidence.cache()

    val herdPoints = evalDataPoints.zip(annotatedEvidence).filter(herdSucceded)

    val nbrEvalDataPoints = evalDataPoints.count()
    val nbrTrueDataPoints = evalDataPoints.filter(_.positive()).count()
    val nbrNegDataPoints = nbrEvalDataPoints - nbrTrueDataPoints

    log.info(s"There are ${nbrTrueDataPoints.toInt} positive examples in the evaluation data")
    log.info(s"There are ${nbrNegDataPoints} negative examples in the evaluation data")

    val herdRecall = herdPoints.count() / nbrEvalDataPoints.toDouble
    log.info(s"Herd successfully found ${herdPoints.count()} target entity pairs, and recall is about: $herdRecall")

    log.info("Testing predictor on test set")
    val predictedRelations = predictor.extractRelations(annotatedEvidence)
      // Filter out the UNKNOWN_CLASS. Keep the empty lists.
      .map(rs => rs.filter(r => !r.predictedPredicate.contains(predictor.UNKNOWN_CLASS))).cache()

    val zippedTestPrediction = evalDataPoints.zip(predictedRelations).cache()

    val nbrPredictedRelations = predictedRelations.map(_.length).reduce(_ + _)
    log.info(s"Extracted ${nbrPredictedRelations} relations from evaluation data")

    log.info("Evaluating the predicted relations")
    val truePositives: RDD[ExtractedRelation] = zippedTestPrediction.flatMap{
      case (datapoint, predRelations) =>
        predRelations.filter(r => isCorrectPrediction(datapoint, r ))
    }.cache()

    val falsePositives: RDD[ExtractedRelation] = zippedTestPrediction.flatMap{
      case (datapoint, predRelations) =>
        predRelations.filter(r => !isCorrectPrediction(datapoint, r))
    }.cache()

    val nbrTruePositives = truePositives.count()
    val nbrFalsePositives = falsePositives.count()
    log.info(s"True Positives: $nbrTruePositives")
    log.info(s"False Positives: $nbrFalsePositives")

    val recall: Double = nbrTruePositives / nbrTrueDataPoints.toDouble
    val precision: Double = nbrTruePositives / nbrPredictedRelations.toDouble
    val f1: Double = computeF1(recall, precision)
    log.info(s"Precision is $precision")
    log.info(s"Recall is $recall")
    log.info(s"F1 is $f1")

    nMostProbable.foreach(N =>
      nMostProbableOutcome(predictedRelations, truePositives, falsePositives, nbrTrueDataPoints, N)
    )
    suggestThreshold(truePositives, falsePositives, nbrTrueDataPoints)

    writeDebug(evalDataPoints, annotatedEvidence, predictedRelations, debugOutFile)

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

  private def herdSucceded(zippedPoint: (EvaluationDataPoint, Document)): Boolean = {
    val dataPoint = zippedPoint._1
    val evidence = zippedPoint._2
    val ents = evidence.nodes(classOf[NamedEntityDisambiguation]).asScala.toSeq.map(_.getIdentifier.split(":").last)
    ents.contains(dataPoint.wd_obj) && ents.contains(dataPoint.wd_sub)
  }

  private def isCorrectPrediction(datapoint: EvaluationDataPoint, r: ExtractedRelation): Boolean = {
    datapoint.wd_obj == r.obj && datapoint.wd_sub == r.subject && datapoint.wd_pred == r.predictedPredicate
  }

  private def computeF1(recall: Double, precision: Double): Double = 2 * (precision * recall) / (precision + recall)

  private def writeDebug(evalDataPoints: RDD[EvaluationDataPoint], annotatedEvidence: RDD[Document],
                         predictedRelations: RDD[Seq[ExtractedRelation]], debugOutFile: Option[String])
                        (implicit sc: SparkContext): Unit = {

    // Save debug information to TSV if debugOutFile supplied
    debugOutFile.foreach(f => {
      val fs = FileSystem.get(sc.hadoopConfiguration)
      val output = fs.create(new Path(f))
      val os = new BufferedOutputStream(output)
      log.info(s"Saving debug information to $f...")
      val data = evalDataPoints.zip(annotatedEvidence).zip(predictedRelations).map{
        case ((evalPoint, annotatedDocument), relations) =>
          val herdResult = herdSucceded(evalPoint, annotatedDocument)
          val relResults = relations.map(r => s"${r.subject}/${r.predictedPredicate}/${r.obj} - ${r.probability} - ${isCorrectPrediction(evalPoint, r)}").mkString("\t")
          s"${evalPoint.evidences.map(_.snippet).mkString(" >> ")}\t${evalPoint.positive()}\t${evalPoint.wd_sub}/${evalPoint.wd_pred}/${evalPoint.wd_obj}\t$herdResult\t$relResults"
      }.collect().mkString("\n")
      os.write("Sentences\tPositive Datapoint\tRDF-triple\tHerd\tPredicted Results:\n".getBytes("UTF-8"))
      os.write(data.getBytes("UTF-8"))
      os.close()
    })
  }

  private def nMostProbableOutcome(predictedRelations: RDD[Seq[ExtractedRelation]],
                                   truePositives: RDD[ExtractedRelation],
                                   falsePositives: RDD[ExtractedRelation],
                                   nbrTrueDataPoints: Double,
                                   n: Int): Unit = {
    val cutOff = predictedRelations
      .flatMap(rs => rs.map(_.probability))
      .takeOrdered(n)(math.Ordering.Double.reverse)
      .last
    log.info(s"cutOff is: $cutOff")
    val trueProb = truePositives.map(_.probability).collect()
    val falseProb = falsePositives.map(_.probability).collect()
    val newTP = trueProb.count(_ >= cutOff)
    val newFP = falseProb.count(_ > cutOff)
    val recall = newTP / nbrTrueDataPoints
    val precision = newTP / (newTP + newFP).toDouble
    val f1 = computeF1(recall, precision)
    log.info(s"\tWith $n most probable => recall: $recall, precision: $precision, f1: $f1")
  }

  private def suggestThreshold(truePositives: RDD[ExtractedRelation], falsePositives: RDD[ExtractedRelation],
                               nbrTrueDataPoints: Double): Unit = {

    val tpCount = truePositives.count().toDouble
    val fpCount = falsePositives.count().toDouble
    val trueProb = truePositives.map(_.probability).collect()
    val falseProb = falsePositives.map(_.probability).collect()
    val meanProbTP = trueProb.sum / tpCount
    val meanProbFP = falseProb.sum / fpCount
    log.info(s"Predictor mean probabilities for TP: $meanProbTP, FP: $meanProbFP")

    if(meanProbFP < meanProbTP) {
      for (cutoff <- meanProbFP to 1.0 by (meanProbTP - meanProbFP) / 20) {
        val newTP = trueProb.count(_ >= cutoff)
        val newFP = falseProb.count(_ >= cutoff)

        val recall = newTP / nbrTrueDataPoints
        val precision = newTP / (newTP + newFP).toDouble
        val f1 = computeF1(recall, precision)
        log.info(s"\tWith probability cutoff $cutoff => recall: $recall, precision: $precision, f1: $f1")
      }
    }
  }

  def save(data: EvaluationResult, path: String)(implicit sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val output = fs.create(new Path(path + ".tsv"))
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
