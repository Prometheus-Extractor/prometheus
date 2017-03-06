package com.sony.prometheus

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import pipeline._


class RelationModelStage(path: String, featureExtractor: Data, featureTransformerStage: Data)
                        (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {

    val data:RDD[TrainingDataPoint] = FeatureExtractor.load(featureExtractor.getData())
    val vocabSize = FeatureTransformer.load(featureTransformerStage.getData()).vocabSize()

    val model = RelationModel(data, vocabSize)
    model.save(path, data.sparkContext)
  }
}

object RelationModel {

  def apply(data: RDD[TrainingDataPoint], vocabSize: Int)(implicit sqlContext: SQLContext): RelationModel = {

    var labeledData = data.map(t => {
      LabeledPoint(t.relationClass.toDouble - 1.0, oneHotEncode(t.features, vocabSize))
    })
    labeledData.cache()


    val classifier = new LogisticRegressionWithLBFGS()
    classifier.setNumClasses(2)
    val model = classifier.run(labeledData)

    new RelationModel(model)
  }

  def load(path: String, context: SparkContext): RelationModel = {
    new RelationModel(LogisticRegressionModel.load(context, path))
  }

  def oneHotEncode(features: Seq[Double], vocabSize: Int): Vector = {
    val f = features.distinct.map(idx => (idx.toInt, 1.0))
    Vectors.sparse(vocabSize, f)
  }

}

class RelationModel(model: LogisticRegressionModel) extends Serializable {

  def save(path: String, context: SparkContext): Unit = {
    model.save(context, path)
  }

  def predict(vector: Vector): Double = {
    model.predict(vector)
  }

  def predict(vectors: RDD[Vector]): RDD[Double] = {
    model.predict(vectors)
  }

}
