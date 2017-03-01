package com.sony.prometheus
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
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

    val tokenEncoder = TokenEncoder(docs)
    new FeatureTransformer(tokenEncoder)

  }

  def load(path: String)(implicit sqlContext: SQLContext): FeatureTransformer = {
    val encoder = TokenEncoder.load(path + "/encoder", sqlContext.sparkContext)
    new FeatureTransformer(encoder)
  }

}

/** Transforms tokens with a [[com.sony.prometheus.TokenEncoder]]
 */
class FeatureTransformer(encoder: TokenEncoder) extends Serializable {

  /** Returns vocabulary size
   */
  def vocabSize(): Int = {
    encoder.vocabSize()
  }

  /** Returns a transformed Seq of tokens as a Seq of Ints with [[com.sony.prometheus.TokenEncoder]]
    * @param tokens - the Seq of Strings to transform
   */
  def transform(tokens: Seq[String]): Seq[Int] = {
    tokens.map(encoder.index)
  }

  /** Saves the feature mapping to the path specified by path
   * @param path - the path to save to
   */
  def save(path: String, sqlContext: SQLContext): Unit = {
    encoder.save(path + "/encoder", sqlContext)
  }

}
