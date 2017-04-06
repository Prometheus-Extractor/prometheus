package com.sony.prometheus.stages

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import com.sony.prometheus.utils.Utils.pathExists

/** Used for creating a FeatureTransformer
 */
object FeatureTransformer {

  def apply(pathToWord2Vec: String, pathToPosEncoder: String)(implicit sqlContext: SQLContext): FeatureTransformer = {
    val posEncoder = StringIndexer.load(pathToPosEncoder, sqlContext.sparkContext)
    val word2vec = Word2VecEncoder.apply(pathToWord2Vec)
    new FeatureTransformer(word2vec, posEncoder)
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

  /** Creates a unified vector with all the features
    * @param  wordFeatures  the word features, a Seq of words
    * @param  posFeatures   the part-of-speech tags for the word features
    * @param  ent1TokensPos the part-of-speech tags for entity1's tokens
    * @param  ent2TokensPos the part-of-speech tags for entity2's tokens
    * @return a unified feature vector
    */
  def toFeatureVector(wordFeatures: Seq[String], posFeatures: Seq[String], ent1TokensPos: Seq[String], ent2TokensPos: Seq[String]): Vector = {
    val wordVectors = wordFeatures.map(wordEncoder.index).map(_.toArray).flatten.toArray
    val posVectors = posFeatures.map(posEncoder.index).map(Seq(_)).map(oneHotEncode(_, posEncoder.vocabSize()).toArray).flatten.toArray

    val ent1Pos = oneHotEncode(    // eg Seq(ADJ, PROPER_NOUN, PROPER_NOUN) repr. (Venerable Barack Obama)
      ent1TokensPos.map(posEncoder.index),  // eg Seq(0, 2, 2) (index of the POS tags)
      posEncoder.vocabSize()
    ).toArray // one-hot encoded, eg Array(1, 0, 1, 0, 0, ... 0) with length posEncoder.vocabSize()

    val ent2Pos = oneHotEncode(
      ent2TokensPos.map(posEncoder.index),
      posEncoder.vocabSize()
    ).toArray

    Vectors.dense(wordVectors ++ posVectors ++ ent1Pos ++ ent2Pos)
  }
}
