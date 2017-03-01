package com.sony.prometheus
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{StringIndexer, StringIndexerModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Token

import scala.collection.JavaConverters._

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

class FeatureTransformer(encoder: TokenEncoder) {

  def vocabSize(): Int = {
    encoder.vocabSize()
  }

  def transform(tokens: Seq[String]): Seq[Int] = {
    tokens.map(encoder.index)
  }

  def save(path: String, sqlContext: SQLContext): Unit = {
    encoder.save(path + "/encoder", sqlContext)
  }

}
