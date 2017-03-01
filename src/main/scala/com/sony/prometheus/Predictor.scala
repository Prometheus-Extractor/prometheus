package com.sony.prometheus

import org.apache.spark.sql.SQLContext

/**
  * Created by erik on 2017-02-28.
  */
object Predictor {

  def apply(modelPath: String, featureExtractorPath: String, featureTransformerPath: String)
           (implicit sqlContext: SQLContext): Predictor = {

    //val model = ModelTrainer.load(modelPath, sqlContext.sparkContext)
    val extractor = FeatureExtractor.load(featureExtractorPath)
    val transformer = FeatureTransformer.load(featureTransformerPath)
    ???
  }

}

class Predictor(model: RelationModel, transformer: FeatureTransformer) {

}
