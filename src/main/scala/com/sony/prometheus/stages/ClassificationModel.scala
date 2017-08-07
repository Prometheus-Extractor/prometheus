package com.sony.prometheus.stages

import java.io.BufferedOutputStream

import com.sony.prometheus.stages.RelationModel.{balanceData, log, splitToTestTrain}
import com.sony.prometheus.utils.Utils.pathExists
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.deeplearning4j.eval.Evaluation
import org.deeplearning4j.nn.api.OptimizationAlgorithm
import org.deeplearning4j.nn.conf.layers.{DenseLayer, OutputLayer}
import org.deeplearning4j.nn.conf.{NeuralNetConfiguration, Updater}
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

/**
  * Trains a classification model
  * @param featureTransformerStage  to provide training data (sentences)
  * @param epochs                   number of epochs to train
  */
class ClassificationModelStage(path: String, featureTransformerStage: FeatureTransformerStage, epochs: Int)
                              (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    if(epochs == 0) {
      LogManager.getLogger(classOf[ClassificationModelStage]).info(s"Epochs set $epochs; skipping.")
    } else {
      val data = FeatureTransformer.load(featureTransformerStage.getData())
      val numClasses = data.take(1)(0).getLabels.length
      val classificationNet = ClassificationModel.trainClassificationNetwork(data, numClasses, epochs)
      ClassificationModel.save(path, classificationNet)
    }
  }
}

object ClassificationModel {

  def trainClassificationNetwork(rawData: RDD[DataSet], numClasses: Int, epochs: Int)
                                (implicit sqlContext: SQLContext): MultiLayerNetwork = {

    val filtered = rawData.filter(_.getLabels().getDouble(0) != 1)
    def getClass = (d: DataSet) => Nd4j.argMax(d.getLabels).getInt(0).toLong

    val (trainData, testData) = splitToTestTrain(filtered, 0.10)
    val balancedTrain = balanceData(trainData, false, getClass)

    //Create the TrainingMaster instance
    val examplesPerDataSetObject = 1
    val trainingMaster = new ParameterAveragingTrainingMaster.Builder(examplesPerDataSetObject)
      .batchSizePerWorker(256) // Up to 2048 is possible on AWS and boosts performance. 256 works on semantica.
      .averagingFrequency(5)
      .workerPrefetchNumBatches(2)
      .rddTrainingApproach(RDDTrainingApproach.Direct)
      .storageLevel(StorageLevel.NONE) // DISK_ONLY or NONE. We have little memory left for caching.
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
      .updater(Updater.ADAM) // test with ADAM?
      //.learningRate(0.005)
      //.momentum(0.9)
      .epsilon(1.0E-8)
      .dropOut(0.5)
      .regularization(true)
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
      sparkNetwork.fit(balancedTrain)
      log.info(s"Epoch finished in ${(System.currentTimeMillis() - start) / 1000}s")

      evaluation = sparkNetwork.evaluate(testData)

      for(i <- (0 until numClasses))
        log.info(s"$i\tRecall: ${evaluation.recall(i)}\t Precision: ${evaluation.precision(i)}\t F1: ${evaluation.f1(i)}")
      log.info(s"T\tRecall: ${evaluation.recall}\t Precision: ${evaluation.precision}\t F1: ${evaluation.f1}\t Acc: ${evaluation.accuracy()}")
      log.info(s"\n${evaluation.confusionToString()}")
    }
    log.info(s"Training done! Network score: ${sparkNetwork.getScore}")
    log.info(sparkNetwork.getNetwork.summary())

    balancedTrain.unpersist(true)
    sparkNetwork.getNetwork
  }

  def save(path: String, net: MultiLayerNetwork)(implicit sqlContext: SQLContext): Unit = {
    val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    val output = fs.create(new Path(path + "/dl4j_model_classify.bin"))
    val os = new BufferedOutputStream(output)
    ModelSerializer.writeModel(net, output, true)
    os.close()
    output.close()
  }

  def load(path: String)(implicit sqlContext: SQLContext): MultiLayerNetwork = {
    val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    val input = fs.open(new Path(path + "/dl4j_model_classify.bin"))
    val classNetwork = ModelSerializer.restoreMultiLayerNetwork(input.getWrappedStream, true)
    input.close()
    log.info(s"Classification model loaded: ${classNetwork.summary()}")
    classNetwork
  }

}
