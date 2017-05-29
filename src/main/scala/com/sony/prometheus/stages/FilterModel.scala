package com.sony.prometheus.stages

import com.sony.prometheus.stages.RelationModel.{log, splitToTestTrain}
import com.sony.prometheus.utils.Utils.pathExists
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.nd4j.linalg.dataset.DataSet

/**
  * Created by erik on 2017-05-29.
  */
class FilterModelStage(path: String, featureTransfomerStage: FeatureTransformerStage)
                      (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val data = FeatureTransformer.load(featureTransfomerStage.getData())
    val filterNet = FilterModel.trainFilterNetwork(data, 10)
    FilterModel.save(path, filterNet)
  }
}

object FilterModel {

  def trainFilterNetwork(rawData: RDD[DataSet], iterations: Int = 10)
                        (implicit sqlContext: SQLContext): LogisticRegressionModel = {

    val labeledData: RDD[LabeledPoint] = rawData.map(d => {
      val label = 1.0 - d.getLabels.getDouble(0)
      val vector = Vectors.dense(d.getFeatures.data().asDouble())
      LabeledPoint(label, vector)
    })
    val (trainData, testData) = splitToTestTrain(labeledData, 0.10)
    trainData.persist(StorageLevel.MEMORY_AND_DISK)

    val classifier = new LogisticRegressionWithLBFGS()
    classifier
      .setNumClasses(2)
      .setIntercept(true)
      .optimizer.setNumIterations(iterations)
    val model = classifier.run(trainData)

    trainData.unpersist(true)
    testData.persist(StorageLevel.DISK_ONLY)

    val defaultThreshold = model.getThreshold
    model.clearThreshold()
    log.info(s"Model threshold was automatically set during training to: $defaultThreshold")

    val predictionWithLabel = model.predict(testData.map(_.features)).zip(testData.map(_.label))
    val metrics = new BinaryClassificationMetrics(predictionWithLabel, 100)

    // Precision-Recall Curve
    val PRC = metrics.pr
    PRC.collect().foreach{ case (p, r) =>
      log.info(s"Precision: $p, Recall: $r")
    }

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.collect().foreach{ case (t, p) =>
      log.info(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.collect().foreach { case (t, r) =>
      log.info(s"Threshold: $t, Recall: $r")
    }

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.collect().foreach { case (t, f) =>
      log.info(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    // AUPRC
    val auPRC = metrics.areaUnderPR
    log.info("Area under precision-recall curve = " + auPRC)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    log.info("Area under ROC = " + auROC)

    testData.unpersist(true)
    metrics.unpersist()
    model.setThreshold(defaultThreshold.get)
    model
  }

  def save(path: String, filterNet: LogisticRegressionModel)(implicit sqlContext: SQLContext): Unit = {
    filterNet.save(sqlContext.sparkContext, path)
  }

  def load(path: String)(implicit sqlContext: SQLContext): LogisticRegressionModel = {
    LogisticRegressionModel.load(sqlContext.sparkContext, path)
  }

}
