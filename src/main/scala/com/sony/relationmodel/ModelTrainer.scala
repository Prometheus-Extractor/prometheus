package com.sony.relationmodel

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}


class ModelTrainerStage(path: String, featureExtractor: Data, featureTransformerStage: Data)
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

    ModelTrainer(data, vocabSize)

  }
}

object ModelTrainer {

  def apply(data: DataFrame, vocabSize: Int)(implicit sqlContext: SQLContext): ModelTrainer = {

    import sqlContext.implicits._
    val labeledData = data.map(row => {

      /* Perform one-hot encoding */
      val features = row.getAs[Seq[Double]](3).distinct.map(idx => (idx.toInt, 1.0))
      (row.getLong(2).toDouble, Vectors.sparse(vocabSize, features))

    }).toDF("label", "features")

    // Set parameters for the algorithm.
    // Here, we limit the number of iterations to 10.
    // instantiate the base classifier
    val classifier = new LogisticRegression()
      .setMaxIter(10)

    val ovr = new OneVsRest()
    ovr.setClassifier(classifier)

    // train the multiclass model.
    val ovrModel = ovr.fit(labeledData)

    new ModelTrainer("ASD")
  }

}

class ModelTrainer(relationId: String) {

}
