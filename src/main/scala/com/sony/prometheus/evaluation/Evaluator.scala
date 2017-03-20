package com.sony.prometheus.evaluation

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext
import com.sony.prometheus.pipeline._
import com.sony.prometheus.Predictor
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.ExtractedRelation
import org.apache.spark.rdd.RDD
import com.sony.prometheus.annotaters.VildeAnnotater


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
    // Evaluator.save(evaluation, path)
  }
}

/** Performs evaluation of [[EvaluationDataPoint]]:s
 */
object Evaluator {
  type EvaluationResult = Tuple3[Double, Double, Double]

  val log = LogManager.getLogger(Evaluator.getClass)

  /** Returns an [[EvaluationResult]]
    * @param evalDataPoints   RDD of [[EvaluationDataPoint]]
    * @returns                a triple (recall, precision, f1)
   */
  def evaluate(evalDataPoints: RDD[EvaluationDataPoint], predictor: Predictor)
    (implicit sqlContext: SQLContext, sc: SparkContext): RDD[EvaluationResult] = {
    // TODO: compute recall, precision, F1 etc between predictions and evalDataPoints

    // Annotate all evidence
    val annotatedEvidence =
      evalDataPoints
      // treat multiple snippets as one string of multiple paragraphs
      .map(dP => dP.evidences.map(_.snippet).mkString("\n"))
      .map(e => VildeAnnotater.annotate(e))
    val predictedRelations = predictor.extractRelations(annotatedEvidence)
    predictedRelations.cache()

    log.info(s"Extracted ${predictedRelations.count()} relations")

    // Evaluate positive examples
    val truePositives = evalDataPoints.zip(predictedRelations)
      .filter{ case (dP, _) =>
        dP.judgments.filter(_.judgment == "yes").length > dP.judgments.length / 2} // majority said yes
      .map{case (dP, rels) =>
        rels.exists(rel => {
          println(s"${dP.wd_pred} ${rel.predictedPredicate}")
          dP.wd_obj == rel.obj && dP.wd_sub == rel.subject && dP.wd_pred == rel.predictedPredicate
        })
      }
      .count()

    log.info(s"truePositives: ${truePositives}")

    val precision = truePositives / predictedRelations.count()

    log.info(s"precision is $precision")

    val evaluation: EvaluationResult = (1,1,1)
    log.info(s"EvaluationResult: $evaluation")
    sc.parallelize(Seq(evaluation))
  }

  def save(data: RDD[EvaluationResult], path: String)(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    data.toDF().write.json(path)
  }
}

