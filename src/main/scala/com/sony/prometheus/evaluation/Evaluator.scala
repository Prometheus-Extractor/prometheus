package com.sony.prometheus.evaluation

import org.apache.spark.SparkContext
import com.sony.prometheus.pipeline._
import com.sony.prometheus.Predictor
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.ExtractedRelation
import org.apache.spark.rdd.RDD

class EvaluatorStage(
  path: String,
  predictor: Data,
  evaluationData: Data)
  (implicit sc: SparkContext, sqlContext: SQLContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path))
      run()
    path
  }

  override def run(): Unit = {
    val predictions = Predictor.load(predictor.getData())
    val trueRelations: RDD[EvaluationDataPoint] = EvaluationDataReader.load(evaluationData.getData())
    val evaluation = Evaluator.evaluate(predictions, trueRelations)
    Evaluator.save(evaluation, path)
  }
}

object Evaluator {
  type EvaluationResult = Tuple3[Double, Double, Double]

  def evaluate(predictions: RDD[ExtractedRelation], trueRelations: RDD[EvaluationDataPoint])
    (implicit sc: SparkContext): RDD[EvaluationResult] = {
    // TODO: compute recall, precision, F1 etc between predictions and trueRelations
    // probably expand this to more granular statistics for each data point
    predictions.map(p => {

    })
    val evaluation: EvaluationResult = (1,1,1)
    sc.parallelize(Seq(evaluation))
  }


  def save(data: RDD[EvaluationResult], path: String)(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    data.toDF().write.json(path)
  }
}

