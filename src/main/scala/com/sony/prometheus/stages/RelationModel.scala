package com.sony.prometheus.stages

import java.io.{BufferedOutputStream, File}

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.utils.Utils.pathExists
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.Updater
import org.deeplearning4j.nn.conf.NeuralNetConfiguration
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.deeplearning4j.nn.weights.WeightInit
import org.deeplearning4j.spark.api.RDDTrainingApproach
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions


/** Builds the RelationModel
 */
class RelationModelStage(path: String, featureTransfomerStage: FeatureTransfomerStage)
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

    val model = RelationModel(data, numClasses)
    model.save(path, data.sparkContext)
  }
}

/** Provides the RelationModel classifier
 */
object RelationModel {

  val ITERATIONS = 1

  def apply(data: RDD[DataSet], numClasses: Int)(implicit sqlContext: SQLContext): RelationModel = {

    var log = LogManager.getLogger(classOf[RelationModel])

    //Create the TrainingMaster instance
    val examplesPerDataSetObject = 1
    val trainingMaster = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)
      .batchSizePerWorker(500)
      .averagingFrequency(10)
      .workerPrefetchNumBatches(2)
      .rddTrainingApproach(RDDTrainingApproach.Direct)
      .storageLevel(StorageLevel.NONE)
      .build()

    val input_size = data.take(1)(0).getFeatures.length
    val output_size = numClasses
    log.info(s"Training NN!")
    log.info(s"Input size: $input_size")
    log.info(s"Output size: $output_size")

    val networkConfig = new NeuralNetConfiguration.Builder()
      .miniBatch(true)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(1) // Not the same as epoch
      .activation(Activation.RELU)
      .weightInit(WeightInit.RELU)
      .learningRate(0.02)
      .updater(Updater.ADAM)
      .dropOut(0.5)
      .useDropConnect(true)
      .list()
      .layer(0, new DenseLayer.Builder().nIn(input_size).nOut(1024).build())
      .layer(1, new DenseLayer.Builder().nIn(1024).nOut(256).build())
      .layer(2, new DenseLayer.Builder().nIn(256).nOut(64).build())
      .layer(3, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .activation(Activation.SOFTMAX).nIn(64).nOut(output_size).build())
      .pretrain(false).backprop(true)
      .build()

    log.info(s"Network: ${networkConfig.toYaml}")

    //Create the SparkDl4jMultiLayer instance
    val sparkNetwork = new SparkDl4jMultiLayer(sqlContext.sparkContext, networkConfig, trainingMaster)
    sparkNetwork.setCollectTrainingStats(false)

    try{
      sparkNetwork.fit(data)
      log.info(s"Training done! Network score: ${sparkNetwork.getScore}")
      log.info(sparkNetwork.getNetwork.summary())
    }finally {
      //trainingMaster.deleteTempFiles(sqlContext.sparkContext)
    }

    new RelationModel(sparkNetwork.getNetwork)
  }

  def load(path: String, context: SparkContext): RelationModel = {
    val fs = FileSystem.get(context.hadoopConfiguration)
    val input = fs.open(new Path(path + "/dl4j_model.bin"))
    val network = ModelSerializer.restoreMultiLayerNetwork(input.getWrappedStream)
    new RelationModel(network)
  }

}

class RelationModel(model: MultiLayerNetwork) extends Serializable {

  def save(path: String, context: SparkContext): Unit = {
    val fs = FileSystem.get(context.hadoopConfiguration)
    val output = fs.create(new Path(path + "/dl4j_model.bin"))
    val os = new BufferedOutputStream(output)
    ModelSerializer.writeModel(model, output, true)
    os.close()
  }

  def predict(vector: Vector): Double = {
    val pred = model.output(Nd4j.create(vector.toArray), false)
    model.predict(Nd4j.create(vector.toArray))(0).toDouble
  }

}
