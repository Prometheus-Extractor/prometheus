package com.sony.prometheus.stages

import java.io.{BufferedOutputStream, PrintWriter}

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.utils.Utils.pathExists
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.storage.StorageLevel
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.Updater
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.spark.api.{RDDTrainingApproach, Repartition}
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions
import java.text.SimpleDateFormat
import java.util.Date

import org.deeplearning4j.eval.Evaluation
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS, SVMWithSGD}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.nd4j.linalg.api.ndarray.INDArray

/** Builds the RelationModel
 */
class RelationModelStage(path: String, featureTransfomerStage: FeatureTransformerStage, epochs: Int)
                        (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val data = FeatureTransformer.load(featureTransfomerStage.getData())
    val numClasses = data.take(1)(0).getLabels.length
    val model = RelationModel(data, numClasses, path, epochs)
  }
}

/** Provides the RelationModel classifier
 */
object RelationModel {

  val log = LogManager.getLogger(classOf[RelationModel])

  def splitToTestTrain[T](data: RDD[T], testPercentage: Double = 0.1): (RDD[T], RDD[T]) = {
    log.info(s"Splitting data into ${1 - testPercentage}:$testPercentage")
    val splits = data.randomSplit(Array(1 - testPercentage, testPercentage))
    (splits(0), splits(1))
  }

  def apply(rawData: RDD[DataSet], numClasses: Int, path: String, epochs: Int)(implicit sqlContext: SQLContext): RelationModel = {

    val labeledData: RDD[LabeledPoint] = rawData.map(d => {
      val label = Nd4j.argMax(d.getLabels).getFloat(0)
      val vector = Vectors.dense(d.getFeatures.data().asDouble())
      LabeledPoint(label, vector)
    })
    val (trainData, testData) = splitToTestTrain(labeledData, 0.10)
    trainData.persist(StorageLevel.MEMORY_AND_DISK)

    val classifier = new LogisticRegressionWithLBFGS()
    classifier
      .setNumClasses(numClasses)
      .setIntercept(true)
      .optimizer.setNumIterations(10)
    val model = classifier.run(trainData)

    val predictionWithLabel = model.predict(testData.map(_.features)).zip(testData.map(_.label))
    val metrics = new MulticlassMetrics(predictionWithLabel)

    // Confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Overall Statistics
    println("Summary Statistics")

    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
    }

    // Weighted stats
    println(s"Weighted precision: ${metrics.weightedPrecision}")
    println(s"Weighted recall: ${metrics.weightedRecall}")
    println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
    println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")


    val relModel = new RelationModel(model)
    save(relModel, sqlContext.sparkContext, null, path, numClasses)
    relModel
  }

  def load(path: String, context: SparkContext): RelationModel = {
    new RelationModel(LogisticRegressionModel.load(context, path))
  }

  def save(relModel: RelationModel, context: SparkContext, evaluation: Evaluation, path: String, numClasses: Int) : Unit = {

    val model = relModel.model
    // Save model
    model.save(context, path)
  }

}

class RelationModel(val model: LogisticRegressionModel) extends Serializable {

  val THRESHOLD = 0.0

  def predict(vector: Vector): Prediction = {
    Prediction(model.predict(vector).toInt, 1.0)
  }
}

case class Prediction(clsIdx: Int, probability: Double)
