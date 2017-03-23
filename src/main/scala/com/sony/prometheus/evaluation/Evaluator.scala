package com.sony.prometheus.evaluation

import java.io.BufferedOutputStream

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import com.sony.prometheus.pipeline._
import com.sony.prometheus.Predictor
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import com.sony.prometheus.annotaters.VildeAnnotater
import org.apache.hadoop.fs.{FileSystem, Path}


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
    if (!exists(path))
      run()
    path
  }

  override def run(): Unit = {
    val evalDataPoints: RDD[EvaluationDataPoint] = EvaluationDataReader.load(evaluationData.getData())
    val evaluation = Evaluator.evaluate(evalDataPoints, predictor)
    Evaluator.save(evaluation, path)
  }
}

/** Performs evaluation of [[EvaluationDataPoint]]:s
 */
object Evaluator {
  case class EvaluationResult(relation: String, recall: Double, precision: Double, f1: Double) {
    override def toString: String = {
      s"""relation\trecall\tprecision\tf1
        |$relation\t$recall\t$precision\t$f1
      """.stripMargin
    }
  }

  val log = LogManager.getLogger(Evaluator.getClass)

  /** Returns an [[EvaluationResult]]
    * @param evalDataPoints   RDD of [[EvaluationDataPoint]]
    * @returns                a triple (recall, precision, f1)
   */
  def evaluate(evalDataPoints: RDD[EvaluationDataPoint], predictor: Predictor)
    (implicit sqlContext: SQLContext, sc: SparkContext): EvaluationResult = {

    evalDataPoints.cache()
    val nbrDataPoints: Double = evalDataPoints.count()
    log.info(s"There are ${nbrDataPoints.toInt} EvaluationDataPoints")
    // Annotate all evidence
    val annotatedEvidence =
      evalDataPoints
      // treat multiple snippets as one string of multiple paragraphs
      .map(dP => dP.evidences.map(_.snippet).mkString("\n"))
      .map(e => VildeAnnotater.annotate(e))
    val predictedRelations = predictor.extractRelations(annotatedEvidence)

    predictedRelations.cache()

    val nbrPredictedRelations: Double = predictedRelations
      .map(rels => rels.filter(!_.predictedPredicate.contains(predictor.UNKNOWN_CLASS)))
      .flatMap(rels => rels)
      .count()
    log.info(s"Extracted ${nbrPredictedRelations.toInt} relations")

    // Evaluate positive examples
    val truePositives = evalDataPoints.zip(predictedRelations)
      .filter{ case (dP, _) =>
        dP.judgments.filter(_.judgment == "yes").length > dP.judgments.length / 2} // majority said yes
      .filter{case (dP, rels) =>
        rels.exists(rel => {
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
    val evaluation: EvaluationResult = EvaluationResult(evalDataPoints.first().wd_pred, recall, precision, f1)
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
}

