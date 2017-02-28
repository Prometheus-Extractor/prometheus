package com.sony.prometheus

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, SQLContext}


class RelationModelStage(path: String, featureExtractor: Data, featureTransformerStage: Data)
                        (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {

    val data:DataFrame = FeatureExtractor.load(featureExtractor.getData())
    val vocabSize = FeatureTransformer.load(featureTransformerStage.getData()).vocabSize()

    val model = RelationModel(data, vocabSize)
    model.save(path, data.sqlContext.sparkContext)
  }
}

object RelationModel {

  def apply(data: DataFrame, vocabSize: Int)(implicit sqlContext: SQLContext): RelationModel = {

    var labeledData = data.map(row => {

      /* Perform one-hot encoding */
      val features = row.getAs[Seq[Double]](3).distinct.map(idx => (idx.toInt, 1.0))
      LabeledPoint(row.getLong(2).toDouble - 1.0, Vectors.sparse(vocabSize, features))

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

}

class RelationModel(model: LogisticRegressionModel) {

  def save(path: String, context: SparkContext): Unit = {
    model.save(context, path)
  }

}
