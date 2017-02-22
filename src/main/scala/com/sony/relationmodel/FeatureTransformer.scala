package com.sony.relationmodel

import org.apache.spark.SparkContext
import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
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
    val model = FeatureTransformer.transform(docs)
    model.save(path)
  }
}

object FeatureTransformer {

  def transform(docs: RDD[Document])(implicit sqlContext: SQLContext): PipelineModel = {

    // Tokenisation
    import sqlContext.implicits._
    val pipeline = new Pipeline("featuretransformer")

    val T = Token.`var`()
    val docsDF = docs.flatMap(doc => {
      doc.nodes(classOf[Token]).asScala.toSeq.map(t => t.text())
    }).filter(Filters.wordFilter)
      .map(token => (token, 1))
      .reduceByKey(_ + _)
      .filter(tup => tup._2 >= 3)
      .map(_._1)
      .toDF("tokens")

    val indexer = new StringIndexer()
      .setInputCol("tokens")
      .setOutputCol("categoryIndex")
      .setHandleInvalid("skip")

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("vector")
    pipeline.setStages(Array(indexer, encoder))
    pipeline.fit(docsDF)
  }
}
