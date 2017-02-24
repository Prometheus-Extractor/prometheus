package com.sony.relationmodel

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{MultiLayerConfiguration, NeuralNetConfiguration}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.optimize.listeners.ScoreIterationListener
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.activations.impl.{ActivationReLU, ActivationSigmoid}
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction


class RelationModelStage(path: String, featureExtractor: Data)
                        (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {

    val data:DataFrame = FeatureExtractor.load(featureExtractor.getData())




    RelationModel(data)



  }
}

object RelationModel {

  def apply(data: DataFrame): RelationModel = {

    data.map(row => {

    })


    // Set parameters for the algorithm.
    // Here, we limit the number of iterations to 10.
    // instantiate the base classifier
    val classifier = new LogisticRegression()
      .setMaxIter(10)

    val ovr = new OneVsRest()
    ovr.setClassifier(classifier)



    // train the multiclass model.
    val ovrModel = ovr.fit(data)

    new RelationModel("ASD")
  }

}

class RelationModel(relationId: String) {

}
