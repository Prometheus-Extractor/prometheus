package com.sony.prometheus.stages

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document

/** Stage in the pipeline for feature transformation
 */
class FeatureTransformerStage(
  path: String,
  corpusData: Data,
  word2vecData: Data)
  (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val docs = CorpusReader.readCorpus(corpusData.getData())
    val word2vec = Word2VecEncoder(word2vecData.getData())
    val model = FeatureTransformer(docs, word2vec)
    model.value.save(path, word2vecData.getData(), sqlContext)
    model.destroy()
  }
}

/** Used for creating a FeatureTransformer
 */
object FeatureTransformer {

  def apply(docs: RDD[Document], tokenEncoder: Word2VecEncoder)(implicit sqlContext: SQLContext): Broadcast[FeatureTransformer] = {
    val posEncoder = TokenEncoder.createPosEncoder(docs)
    sqlContext.sparkContext.broadcast(new FeatureTransformer(tokenEncoder, posEncoder))
  }

  def load(path: String)(implicit sqlContext: SQLContext): Broadcast[FeatureTransformer] = {
    val word2vec = sqlContext.sparkContext.textFile(path + "/word2vec").collect().mkString
    val posEncoder = TokenEncoder.load(path + "/pos_encoder", sqlContext.sparkContext)
    sqlContext.sparkContext.broadcast(new FeatureTransformer(Word2VecEncoder(word2vec), posEncoder))
  }

}

/** Transforms tokens with a [[stages.TokenEncoder]]
 */
class FeatureTransformer(val wordEncoder: Word2VecEncoder, val posEncoder: TokenEncoder) extends Serializable {

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
