package com.sony.prometheus.stages

import com.sony.prometheus._
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.utils.Utils.pathExists
import org.apache.spark.storage.StorageLevel


/** Builds the RelationModel
 */
class RelationModelStage(path: String, featureExtractor: FeatureExtractorStage, word2VecData: Word2VecData, posEncoder: PosEncoderStage)
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
    val labeledData = data.map(t => {
      val vec = broadcastedFT.value.toFeatureVector(t.wordFeatures, t.posFeatures, t.ent1PosTags, t.ent2PosTags)
      LabeledPoint(t.relationClass, vec)
    }).persist(StorageLevel.MEMORY_AND_DISK)

    val classifier = new LogisticRegressionWithLBFGS()
    classifier
      .setNumClasses(numClasses)
      .setIntercept(true)
      .optimizer.setNumIterations(MAX_ITERATIONS)
    val model = classifier.run(labeledData)

    labeledData.unpersist(true)
    new RelationModel(model)
  }

  def load(path: String, context: SparkContext): RelationModel = {
    new RelationModel(LogisticRegressionModel.load(context, path))
  }

}

class RelationModel(model: LogisticRegressionModel) extends Serializable {

  def save(path: String, context: SparkContext): Unit = {
    model.save(context, path)
  }

  def predict(vector: Vector): Double = {
    model.predict(vector)
  }

  def predict(vectors: RDD[Vector]): RDD[Double] = {
    model.predict(vectors)
  }

}
