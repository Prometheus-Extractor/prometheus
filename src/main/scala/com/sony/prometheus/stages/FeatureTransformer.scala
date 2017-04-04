package com.sony.prometheus.stages

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import com.sony.prometheus.utils.Utils.pathExists

class FeatureTransformerStage(path: String, featureExtractor: Data, word2vecData: Data, posEncoder: Data)
                             (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {

    // This code is run at the master because serializing the word2vec model is problematic
    val data: RDD[TrainingDataPoint] = FeatureExtractor.load(featureExtractor.getData())
    val featureTransformer = sc.broadcast(FeatureTransformer(word2vecData.getData(), posEncoder.getData()))

    import sqlContext.implicits._
    data.map(d => {
      TransformedFeature(d.relationClass, featureTransformer.value.toFeatureVector(d.wordFeatures, d.posFeatures))
    }).toDF().write.parquet(path)


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

  /** Loads the data from path
    */
  def load(path: String)(implicit sqlContext: SQLContext): RDD[TransformedFeature]  = {
    import sqlContext.implicits._
    sqlContext.read.parquet(path).as[TransformedFeature].rdd
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

  private def oneHotEncode(features: Seq[Int], vocabSize: Int): Vector = {
    val f = features.distinct.map(idx => (idx, 1.0))
    Vectors.sparse(vocabSize, f)
  }

  /** Saves the feature mapping to the path specified by path
   * @param path - the path to save to
   */
  def save(path: String, word2vecPath: String, sqlContext: SQLContext): Unit = {
    sqlContext.sparkContext.parallelize(List(word2vecPath)).repartition(1).saveAsTextFile(path + "/word2vec")
    posEncoder.save(path + "/pos_encoder", sqlContext)
  }

  /** Creates a unified vector with
    */
  def toFeatureVector(wordFeatures: Seq[String], posFeatures: Seq[String]): Vector = {
    val wordVectors = wordFeatures.map(wordEncoder.index).map(_.toArray).flatten.toArray
    val posVectors = posFeatures.map(posEncoder.index).map(Seq(_)).map(oneHotEncode(_, posEncoder.vocabSize()).toArray).flatten.toArray
    Vectors.dense(wordVectors ++ posVectors)

  }

}

case class TransformedFeature(classIdx: Long, featureVector: Vector)
