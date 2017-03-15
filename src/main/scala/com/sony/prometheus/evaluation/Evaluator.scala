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
    Evaluator.save(evaluation, path)
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
    val accum = sc.accumulator(0, "True Positives")
    val truePositives =
      evalDataPoints // RDD[EDP]
      .filter(dP => dP.judgments.length == dP.judgments.filter(_.judgment == "yes").length)
      .map(dP => // RDD[(EDP, Seq[RDD[annotatedDoc1], RDD[annotatedDoc2]])]
        (dP, dP.evidences.map(_.snippet).map(s => VildeAnnotater.annotate(s)).map(d => sc.parallelize(Seq(d)))))
      .flatMap{case (dP, docs) => {
        docs.map(doc => {
          log.debug(s"doc: $doc")
          val p = predictor.extractRelations(doc).filter(rel => {
            accum += 1
            dP.wd_sub == rel.subject && dP.wd_obj == rel.obj &&
            dP.pred == rel.predictedPredicate
          }).count()
          p
        })
      }}.count()
    log.info(s"True Positives: $truePositives")
    log.info(s"Accum: $accum")
    val evaluation: EvaluationResult = (1,1,1)
    log.info(s"EvaluationResult: $evaluation")
    sc.parallelize(Seq(evaluation))
  }

  def save(data: RDD[EvaluationResult], path: String)(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    data.toDF().write.json(path)
  }
}

