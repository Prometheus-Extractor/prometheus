package com.sony.prometheus.stages

import java.io.{BufferedOutputStream, File, PrintWriter}

import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
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
import org.deeplearning4j.spark.api.RDDTrainingApproach
import org.deeplearning4j.spark.impl.multilayer.SparkDl4jMultiLayer
import org.deeplearning4j.spark.impl.paramavg.ParameterAveragingTrainingMaster
import org.deeplearning4j.util.ModelSerializer
import org.nd4j.linalg.activations.Activation
import org.nd4j.linalg.dataset.DataSet
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.lossfunctions.LossFunctions
import java.text.SimpleDateFormat
import java.util.Date

import com.sony.prometheus.Prometheus
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.optimize.listeners.ScoreIterationListener


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

  val log = LogManager.getLogger(classOf[RelationModel])

  val NUM_EPOCHS = 10

  def splitToTestTrain(data: RDD[DataSet], testPercentage: Double = 0.1): (RDD[DataSet], RDD[DataSet]) = {
    log.info(s"Splitting data into ${1 - testPercentage}:$testPercentage")
    val splits = data.randomSplit(Array(1 - testPercentage, testPercentage))
    (splits(0), splits(1))
  }

  def apply(rawData: RDD[DataSet], numClasses: Int)(implicit sqlContext: SQLContext): RelationModel = {

    val (trainData, testData) = splitToTestTrain(rawData, 0.05)

    //Create the TrainingMaster instance
    val examplesPerDataSetObject = 1
    val trainingMaster = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)
      .batchSizePerWorker(256)
      .averagingFrequency(10)
      .workerPrefetchNumBatches(2)
      .rddTrainingApproach(RDDTrainingApproach.Direct)
      .storageLevel(StorageLevel.MEMORY_AND_DISK_SER)
      .build()

    val input_size = trainData.take(1)(0).getFeatures.length
    val output_size = numClasses

    val networkConfig = new NeuralNetConfiguration.Builder()
      .miniBatch(true)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(1) // Not the same as epoch. Should probably only ever be 1.
      .activation(Activation.RELU)
      .weightInit(WeightInit.XAVIER)
      .learningRate(0.005)
      .updater(Updater.NESTEROVS)
      .momentum(0.9)
      .dropOut(0.5)
      .useDropConnect(true)
      .list()
      .layer(0, new DenseLayer.Builder().nIn(input_size).nOut(256).build())
      .layer(1, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .activation(Activation.SOFTMAX).nIn(256).nOut(output_size).build())
      .pretrain(true).backprop(true)
      .build()

    log.info(s"Training Network, in: $input_size out: $output_size")
    log.debug(s"Network: ${networkConfig.toYaml}")

    //Create the SparkDl4jMultiLayer instance
    val sparkNetwork = new SparkDl4jMultiLayer(sqlContext.sparkContext, networkConfig, trainingMaster)
    sparkNetwork.setCollectTrainingStats(false) // Not that useful info. Mostly times for steps.

    for(i <- (1 to NUM_EPOCHS)){
      log.info(s"Epoch: $i/$NUM_EPOCHS")
      sparkNetwork.fit(trainData)
      val evaluation = sparkNetwork.evaluate(testData)
      log.info(evaluation.stats)
      log.info(s"\n${evaluation.confusionToString()}")
    }
    log.info(s"Training done! Network score: ${sparkNetwork.getScore}")
    log.info(sparkNetwork.getNetwork.summary())

    new RelationModel(sparkNetwork.getNetwork)
  }

  def load(path: String, context: SparkContext): RelationModel = {
    val fs = FileSystem.get(context.hadoopConfiguration)
    val input = fs.open(new Path(path + "/dl4j_model.bin"))
    val network = ModelSerializer.restoreMultiLayerNetwork(input.getWrappedStream, true)
    new RelationModel(network)
  }

}

class RelationModel(model: MultiLayerNetwork) extends Serializable {

  def save(path: String, context: SparkContext): Unit = {

    // Save model
    val fs = FileSystem.get(context.hadoopConfiguration)
    var output = fs.create(new Path(path + "/dl4j_model.bin"))
    var os = new BufferedOutputStream(output)
    ModelSerializer.writeModel(model, output, true)
    os.close()
    output.close()

    // Save
    output = fs.create(new Path(path + "/model_info.txt"))
    os = new BufferedOutputStream(output)
    var pw = new PrintWriter(os)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    pw.write(s"Created: ${sdf.format(new Date())}")
    pw.write("Model Info:")
    pw.write(s"Score: ${model.score}")
    pw.write("Summary:")
    pw.write(model.summary())
    pw.write("Config:")
    pw.write(model.conf().toJson)
    pw.close()
    os.close()
    output.close()

  }

  def predict(vector: Vector): Double = {
    //val pred = model.output(Nd4j.create(vector.toArray), false)
    //LogManager.getLogger(classOf[RelationModel]).info("Prediction:" + (0 until OUTPUT_SIZE).map(pred.getFloat(_)).mkString(","))
    model.predict(Nd4j.create(vector.toArray))(0).toDouble
  }

}
