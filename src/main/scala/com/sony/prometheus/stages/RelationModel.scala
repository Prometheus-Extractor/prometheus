package com.sony.prometheus.stages

import java.io.{BufferedOutputStream, PrintWriter}

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

import com.sony.prometheus.Prometheus
import org.deeplearning4j.eval.Evaluation

import scala.util.Random

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

  def splitToTestTrain(data: RDD[DataSet], testPercentage: Double = 0.1): (RDD[DataSet], RDD[DataSet]) = {
    log.info(s"Splitting data into ${1 - testPercentage}:$testPercentage")
    val splits = data.randomSplit(Array(1 - testPercentage, testPercentage))
    (splits(0), splits(1))
  }

  def balanceData[T](rawData: RDD[T], underSample: Boolean = false, getClass: (T) => Long): RDD[T] = {

    val classCount = rawData.map(getClass(_)).countByValue()
    val posClasses = classCount.filter(_._1 != FeatureExtractor.NEGATIVE_CLASS_NBR)
    val sampleTo = if (underSample) posClasses.map(_._2).min else posClasses.map(_._2).max

    log.info(s"Rebalancing dataset (${if (underSample) "undersample" else "oversample"})")
    classCount.foreach(pair => log.info(s"\tClass ${pair._1}: ${pair._2}"))

    /* Resample postive classes */
    val balancedDataset = classCount.map{
      case (key: Long, count: Long) =>
        val samplePercentage = sampleTo / count.toDouble
        val replacement = sampleTo > count
        rawData.filter(getClass(_) == key).sample(replacement, samplePercentage)

    }.reduce(_++_)

    log.info("Balanced result:")
    balancedDataset.map(getClass(_)).countByValue().foreach(pair => log.info(s"\tClass ${pair._1}: ${pair._2}"))
    balancedDataset //.repartition(Prometheus.DATA_PARTITIONS)
  }

  def apply(rawData: RDD[DataSet], numClasses: Int, path: String, epochs: Int)(implicit sqlContext: SQLContext): RelationModel = {

    val filterNet = trainFilterNetwork(rawData, epochs)
    val classificationNet = trainClassificationNetwork(rawData, numClasses, epochs)
    val relModel = new RelationModel(filterNet, classificationNet)
    if(path != "")
      save(relModel, sqlContext.sparkContext, path, numClasses)

    relModel
  }

  def trainFilterNetwork(rawData: RDD[DataSet], epochs: Int)
                        (implicit sqlContext: SQLContext): MultiLayerNetwork = {

    val binaryData = rawData.map(d => {
      d.setLabels(Nd4j.create(Array(1.0 - d.getLabels.getDouble(0))))
      d
    })
    def getClass = (d: DataSet) => d.getLabels.getDouble(0).toLong
    val balanced = balanceData(binaryData, false, getClass)

    val (trainData, testData) = splitToTestTrain(balanced, 0.10)

    //Create the TrainingMaster instance
    val examplesPerDataSetObject = 1
    val trainingMaster = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)
      .batchSizePerWorker(256) // Up to 2048 is possible on AWS and boosts performance. 256 works on semantica.
      .averagingFrequency(5)
      .workerPrefetchNumBatches(2)
      .rddTrainingApproach(RDDTrainingApproach.Direct)
      .storageLevel(StorageLevel.DISK_ONLY) // DISK_ONLY or NONE. We have little memory left for caching.
      .repartionData(Repartition.Always) // Should work with never because we repartitioned the dataset before storing. Default is always
      .build()

    val input_size = rawData.take(1)(0).getFeatures.length
    val output_size = 1

    val networkConfig = new NeuralNetConfiguration.Builder()
      .miniBatch(true)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(1) // Not the same as epoch. Should probably only ever be 1.
      .activation(Activation.RELU)
      .weightInit(WeightInit.XAVIER)
      .updater(Updater.ADADELTA)
      //.learningRate(0.005)
      //.momentum(0.9)
      .epsilon(1.0E-8)
      .dropOut(0.5)
      .list()
      .layer(0, new DenseLayer.Builder().nIn(input_size).nOut(512).build())
      .layer(1, new DenseLayer.Builder().nIn(512).nOut(256).build())
      .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.XENT)
        .activation(Activation.SIGMOID).nIn(256).nOut(output_size).build())
      .pretrain(false).backprop(true)
      .build()

    log.info(s"Training Network, in: $input_size out: $output_size")
    log.debug(s"Network: ${networkConfig.toYaml}")

    //Create the SparkDl4jMultiLayer instance
    val sparkNetwork = new SparkDl4jMultiLayer(sqlContext.sparkContext, networkConfig, trainingMaster)

    var evaluation: Evaluation = null
    for(i <- (1 to epochs)){
      log.info(s"Epoch: $i/$epochs")
      val start = System.currentTimeMillis
      sparkNetwork.fit(trainData)
      log.info(s"Epoch finished in ${(System.currentTimeMillis() - start) / 1000}s")

      evaluation = sparkNetwork.evaluate(testData)

      log.info(s"T\tRecall: ${evaluation.recall}\t Precision: ${evaluation.precision}\t F1: ${evaluation.f1}\t Acc: ${evaluation.accuracy()}")
      log.info(s"\n${evaluation.confusionToString()}")
    }
    log.info(s"Training done! Network score: ${sparkNetwork.getScore}")
    log.info(sparkNetwork.getNetwork.summary())

    trainData.unpersist(false)
    sparkNetwork.getNetwork
  }

  def trainClassificationNetwork(rawData: RDD[DataSet], numClasses: Int, epochs: Int)
                                (implicit sqlContext: SQLContext): MultiLayerNetwork = {

    val filtered = rawData.filter(_.getLabels().getDouble(0) != 1)
    def getClass = (d: DataSet) => Nd4j.argMax(d.getLabels).getInt(0).toLong
    val balanced = balanceData(filtered, false, getClass)

    val (trainData, testData) = splitToTestTrain(balanced, 0.10)

    //Create the TrainingMaster instance
    val examplesPerDataSetObject = 1
    val trainingMaster = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)
      .batchSizePerWorker(256) // Up to 2048 is possible on AWS and boosts performance. 256 works on semantica.
      .averagingFrequency(5)
      .workerPrefetchNumBatches(2)
      .rddTrainingApproach(RDDTrainingApproach.Direct)
      .storageLevel(StorageLevel.DISK_ONLY) // DISK_ONLY or NONE. We have little memory left for caching.
      .repartionData(Repartition.Always) // Should work with never because we repartitioned the dataset before storing. Default is always
      .build()

    val input_size = rawData.take(1)(0).getFeatures.length
    val output_size = numClasses

    val networkConfig = new NeuralNetConfiguration.Builder()
      .miniBatch(true)
      .optimizationAlgo(OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT)
      .iterations(1) // Not the same as epoch. Should probably only ever be 1.
      .activation(Activation.RELU)
      .weightInit(WeightInit.XAVIER)
      .updater(Updater.ADADELTA)
      //.learningRate(0.005)
      //.momentum(0.9)
      .epsilon(1.0E-8)
      .dropOut(0.5)
      .list()
      .layer(0, new DenseLayer.Builder().nIn(input_size).nOut(512).build())
      .layer(1, new DenseLayer.Builder().nIn(512).nOut(256).build())
      .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.MCXENT)
        .activation(Activation.SOFTMAX).nIn(256).nOut(output_size).build())
      .pretrain(false).backprop(true)
      .build()

    log.info(s"Training Classification Network, in: $input_size out: $output_size")
    log.debug(s"Network: ${networkConfig.toYaml}")

    //Create the SparkDl4jMultiLayer instance
    val sparkNetwork = new SparkDl4jMultiLayer(sqlContext.sparkContext, networkConfig, trainingMaster)

    var evaluation: Evaluation = null
    for(i <- (1 to epochs)){
      log.info(s"Epoch: $i/$epochs")
      val start = System.currentTimeMillis
      sparkNetwork.fit(trainData)
      log.info(s"Epoch finished in ${(System.currentTimeMillis() - start) / 1000}s")

      evaluation = sparkNetwork.evaluate(testData)

      for(i <- (0 until numClasses))
        log.info(s"$i\tRecall: ${evaluation.recall(i)}\t Precision: ${evaluation.precision(i)}\t F1: ${evaluation.f1(i)}")
      log.info(s"T\tRecall: ${evaluation.recall}\t Precision: ${evaluation.precision}\t F1: ${evaluation.f1}\t Acc: ${evaluation.accuracy()}")
      log.info(s"\n${evaluation.confusionToString()}")
    }
    log.info(s"Training done! Network score: ${sparkNetwork.getScore}")
    log.info(sparkNetwork.getNetwork.summary())

    trainData.unpersist(false)
    sparkNetwork.getNetwork
  }

  def load(path: String, context: SparkContext): RelationModel = {
    val fs = FileSystem.get(context.hadoopConfiguration)
    var input = fs.open(new Path(path + "/dl4j_model_filter.bin"))
    val filterNetwork = ModelSerializer.restoreMultiLayerNetwork(input.getWrappedStream, true)
    input.close()

    input = fs.open(new Path(path + "/dl4j_model_classify.bin"))
    val classNetwork = ModelSerializer.restoreMultiLayerNetwork(input.getWrappedStream, true)
    new RelationModel(filterNetwork, classNetwork)
  }

  def save(relModel: RelationModel, context: SparkContext, path: String, numClasses: Int) : Unit = {

    // Save model
    val fs = FileSystem.get(context.hadoopConfiguration)
    var output = fs.create(new Path(path + "/dl4j_model_filter.bin"))
    var os = new BufferedOutputStream(output)
    ModelSerializer.writeModel(relModel.filterModel, output, true)
    os.close()
    output.close()

    output = fs.create(new Path(path + "/dl4j_model_classify.bin"))
    os = new BufferedOutputStream(output)
    ModelSerializer.writeModel(relModel.classModel, output, true)
    os.close()
    output.close()

    // Save
    /*
    output = fs.create(new Path(path + "/model_info.txt"))
    os = new BufferedOutputStream(output)
    var pw = new PrintWriter(os)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    pw.println(s"Created: ${sdf.format(new Date())}")
    pw.println("Model Info:")
    pw.println(s"Score: ${model.score}")
    pw.println("Summary:")
    pw.println(model.summary())
    pw.println("Config:")
    pw.println(model.conf().toJson)
    */
    /*
    for(i <- (0 until numClasses))
      pw.println(s"$i\tRecall: ${evaluation.recall(i)}\t Precision: ${evaluation.precision(i)}\t F1: ${evaluation.f1(i)}")
    pw.println(s"T\tRecall: ${evaluation.recall}\t Precision: ${evaluation.precision}\t F1: ${evaluation.f1}\t Acc: ${evaluation.accuracy()}")
    pw.println(s"\n${evaluation.confusionToString()}")

    pw.close()
    os.close()
    output.close()
    */
  }

}

class RelationModel(val filterModel: MultiLayerNetwork, val classModel: MultiLayerNetwork) extends Serializable {

  val THRESHOLD = 0.0

  def predict(vector: Vector): Prediction = {
    val vec = Nd4j.create(vector.toArray)

    val isRelation = filterModel.output(vec, false).getDouble(0)

    if(isRelation <= 0.5){
      Prediction(0, 1.0 - isRelation)
    } else {
      val cls = classModel.predict(vec)(0)
      val prob = classModel.output(vec, false).getDouble(cls)
      if (prob >= THRESHOLD) {
        Prediction(cls, prob)
      } else {
        Prediction(0, prob)
      }
    }
  }
}

case class Prediction(clsIdx: Int, probability: Double)
