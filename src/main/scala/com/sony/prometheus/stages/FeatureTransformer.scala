package com.sony.prometheus.stages

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import com.sony.prometheus.utils.Utils.pathExists
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.dataset.DataSet

class FeatureTransfomerStage(path: String, word2VecData: Word2VecData, posEncoderStage: PosEncoderStage,
                             featureExtractorStage: FeatureExtractorStage)
                            (implicit sqlContext:SQLContext, sparkContext: SparkContext) extends Task with Data{
  /** Runs the task, saving results to disk
    */
  override def run(): Unit = {
    val featureTransformer = sqlContext.sparkContext.broadcast(
      FeatureTransformer(word2VecData.getData(), posEncoderStage.getData()))
    val data = FeatureExtractor.load(featureExtractorStage.getData())
    val numClasses = data.map(d => d.relationClass).distinct().count().toInt

    data.map(d => {
      val vector = featureTransformer.value.toFeatureVector(d.wordFeatures, d.posFeatures).toArray.map(_.toFloat)
      val features = Nd4j.create(vector)
      val label = Nd4j.create(featureTransformer.value.oneHotEncode(Seq(d.relationClass.toInt), numClasses).toArray)
      new DataSet(features, label)

    }).saveAsObjectFile(path)
    featureTransformer.destroy()

  }

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

}

/** Used for creating a FeatureTransformer
 */
object FeatureTransformer {

  def apply(pathToWord2Vec: String, pathToPosEncoder: String)(implicit sqlContext: SQLContext): FeatureTransformer = {
    val posEncoder = StringIndexer.load(pathToPosEncoder, sqlContext.sparkContext)
    val word2vec = Word2VecEncoder.apply(pathToWord2Vec)
    new FeatureTransformer(word2vec, posEncoder)
  }

  def load(path: String)(implicit sqlContext: SQLContext): RDD[DataSet] = {
    sqlContext.sparkContext.objectFile[DataSet](path)
  }

}

/** Transforms tokens with a [[stages.TokenEncoder]]
 */
class FeatureTransformer(val wordEncoder: Word2VecEncoder, val posEncoder: StringIndexer) extends Serializable {

  /** Returns a transformed Seq of tokens as a Seq of Ints with [[stages.TokenEncoder]]
    *
    * @param tokens - the Seq of Strings to transform
   */
  def transformWords(tokens: Seq[String]): Seq[Vector] = {
    tokens.map(wordEncoder.index)
  }

  def transformPos(pos: Seq[String]): Seq[Int] = {
    pos.map(posEncoder.index)
  }

  def oneHotEncode(features: Seq[Int], vocabSize: Int): Vector = {
    val f = features.distinct.map(idx => (idx, 1.0))
    Vectors.sparse(vocabSize, f)
  }

  /** Creates a unified vector with
    */
  def toFeatureVector(wordFeatures: Seq[String], posFeatures: Seq[String]): Vector = {
    val wordVectors = wordFeatures.map(wordEncoder.index).map(_.toArray).flatten.toArray
    val posVectors = posFeatures.map(posEncoder.index).map(Seq(_)).map(oneHotEncode(_, posEncoder.vocabSize()).toArray).flatten.toArray
    Vectors.dense(wordVectors ++ posVectors)
  }

}

//case class VectorizedTrainingPoint(label: Array[Float], dataset: Array[Float])
