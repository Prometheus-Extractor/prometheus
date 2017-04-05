package com.sony.prometheus.stages

import java.io.File

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.utils.Utils.pathExists
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.Updater
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions


/** Builds the RelationModel
 */
class RelationModelStage(path: String, featureExtractor: Data, word2VecData: Data, posEncoder: Data)
                        (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {

    val data = FeatureExtractor.load(featureExtractor.getData())
    val numClasses = data.map(d => d.relationClass).distinct().count().toInt
    val featureTransformer = FeatureTransformer(word2VecData.getData(), posEncoder.getData())

    val model = RelationModel(data, numClasses, featureTransformer)
    model.save(path, data.sparkContext)
  }
}

/** Provides the RelationModel classifier
 */
object RelationModel {

  val MAX_ITERATIONS = 10

  def printDataInfo(data: RDD[TrainingDataPoint], vocabSize: Int, numClasses: Int): Unit = {
    val log = LogManager.getLogger(RelationModel.getClass)
    log.info("Training Model")
    log.info(s"Vocab size: $vocabSize")
    log.info(s"Number of classes: $numClasses")
    log.info("Data distribution:")
    data.map(t => (t.relationId, 1)).reduceByKey(_+_).map(t=> s"${t._2}\t${t._1}").collect().map(log.info)
  }

  def apply(data: RDD[TrainingDataPoint], numClasses: Int, featureTransformer: FeatureTransformer)(implicit sqlContext: SQLContext): RelationModel = {

    val broadcastedFT = sqlContext.sparkContext.broadcast(featureTransformer)

    val labeledData:RDD[DataSet] = data.map(t => {
      val vec = broadcastedFT.value.toFeatureVector(t.wordFeatures, t.posFeatures)
      val features = Nd4j.create(vec.toArray)
      // One hot encode the labels as well
      val label = Nd4j.create(broadcastedFT.value.oneHotEncode(Seq(t.relationClass.toInt), numClasses).toArray)
      new DataSet(features, label)
    }) // Do not persist/cache a RDD[Dataset]. This is done by ND4J internally.

    //Create the TrainingMaster instance
    val examplesPerDataSetObject = 1
    val trainingMaster = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)
      .batchSizePerWorker(2000)
      //.averagingFrequency()
      .workerPrefetchNumBatches(3)
      //.saveUpdater()
      //.repartition()
      .build();

    val input_size = data.take(1).map(t => broadcastedFT.value.toFeatureVector(t.wordFeatures, t.posFeatures).size).apply(0)
    val output_size = numClasses

    val networkConfig = new NeuralNetConfiguration.Builder()
      .miniBatch(true)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT).iterations(1)
      .activation(Activation.RELU)
      .weightInit(WeightInit.XAVIER)
      .learningRate(0.02)
      .updater(Updater.NESTEROVS).momentum(0.9)
      .regularization(true).l2(1e-4)
      .list()
      .layer(0, new DenseLayer.Builder().nIn(input_size).nOut(4096).build())
      .layer(1, new DenseLayer.Builder().nIn(4096).nOut(1024).build())
      .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.NEGATIVELOGLIKELIHOOD)
        .activation(Activation.SOFTMAX).nIn(1024).nOut(output_size).build())
      .pretrain(false).backprop(true)
      .build();

    //Create the SparkDl4jMultiLayer instance
    val sparkNetwork = new SparkDl4jMultiLayer(sqlContext.sparkContext, networkConfig, trainingMaster)
    sparkNetwork.setCollectTrainingStats(false)

    //Fit the network using the training data:
    sparkNetwork.fit(labeledData)

    // cleanup
    trainingMaster.deleteTempFiles(sqlContext.sparkContext)
    broadcastedFT.destroy()

    println(s"Network score: ${sparkNetwork.getScore}")
    new RelationModel(sparkNetwork)
  }

  def load(path: String, context: SparkContext): RelationModel = {
    val network = ModelSerializer.restoreMultiLayerNetwork(new File("/home/ine11ega/dl4j_model.zip"))
    //new SparkDl4jMultiLayer(context, )
    //new RelationModel(LogisticRegressionModel.load(context, path))
    val sparkNetwork = new SparkDl4jMultiLayer(context, network, null)
    new RelationModel(sparkNetwork)
  }

}

class RelationModel(model: SparkDl4jMultiLayer) extends Serializable {

  def save(path: String, context: SparkContext): Unit = {
    ModelSerializer.writeModel(model.getNetwork, new File("/home/ine11ega/dl4j_model.zip"), true)
  }

  def predict(vector: Vector): Double = {
    model.predict(vector).argmax.toDouble
  }

  def predict(vectors: RDD[Vector]): RDD[Double] = {
    vectors.map(v => {
      model.predict(v).argmax.toDouble
    })
  }

}
