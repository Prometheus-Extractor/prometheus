package com.sony.prometheus
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Token
import pipeline._

import scala.collection.JavaConverters._

/** Stage in the pipeline for feature transformation
 */
class FeatureTransformerStage(
  path: String,
  corpusData: Data)
  (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val docs = CorpusReader.readCorpus(corpusData.getData())
    val model = FeatureTransformer(docs)
    model.save(path, sqlContext)
  }
}

/** Used for creating a FeatureTransformer
 */
object FeatureTransformer {

  def apply(docs: RDD[Document])(implicit sqlContext: SQLContext): FeatureTransformer = {

    val tokenEncoder = TokenEncoder.createWordEncoder(docs)
    val posEncoder = TokenEncoder.createPosEncoder(docs)
    new FeatureTransformer(tokenEncoder, posEncoder)

  }

  def load(path: String)(implicit sqlContext: SQLContext): FeatureTransformer = {
    val wordEncoder = TokenEncoder.load(path + "/word_encoder", sqlContext.sparkContext)
    val posEncoder = TokenEncoder.load(path + "/pos_encoder", sqlContext.sparkContext)
    new FeatureTransformer(wordEncoder, posEncoder)
  }

  /**
    * Performs one-hot encoding from of sequences of doubles that represent indexes in a sparse vector.
    */
  def oneHotEncode(features: Seq[Double], vocabSize: Int): Vector = {
    val f = features.distinct.map(idx => (idx.toInt, 1.0))
    Vectors.sparse(vocabSize, f)
  }

}

/** Transforms tokens with a [[com.sony.prometheus.TokenEncoder]]
 */
class FeatureTransformer(val wordEncoder: TokenEncoder, val posEncoder: TokenEncoder) extends Serializable {

  /** Returns a transformed Seq of tokens as a Seq of Ints with [[com.sony.prometheus.TokenEncoder]]
    * @param tokens - the Seq of Strings to transform
   */
  def transformWords(tokens: Seq[String]): Seq[Int] = {
    tokens.map(wordEncoder.index)
  }

  def transformPos(pos: Seq[String]): Seq[Int] = {
    pos.map(posEncoder.index)
  }

  def combinedFeatureSize(): Int = {
    wordEncoder.vocabSize + posEncoder.vocabSize
  }

  /** Saves the feature mapping to the path specified by path
   * @param path - the path to save to
   */
  def save(path: String, sqlContext: SQLContext): Unit = {
    wordEncoder.save(path + "/word_encoder", sqlContext)
    posEncoder.save(path + "/pos_encoder", sqlContext)
  }

  /** Creates a unified vector with [one-hot bag of words, one-hot pos1, one-hot pos2> ...]
    */
  def toFeatureVector(wordFeatures: Seq[Double], posFeatures: Seq[Double]): Vector = {
    val vocabSize = wordEncoder.vocabSize() + posFeatures.length * posEncoder.vocabSize()
    val indexes: Seq[Double] = wordFeatures ++ posFeatures.zipWithIndex.map(p => wordFeatures.length + p._2 * posFeatures.length + p._1)
    FeatureTransformer.oneHotEncode(indexes, vocabSize)
  }

}
