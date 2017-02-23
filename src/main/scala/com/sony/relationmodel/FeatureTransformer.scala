package com.sony.relationmodel

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
    model.save(path)
  }
}

object FeatureTransformer {

  def apply(docs: RDD[Document])(implicit sqlContext: SQLContext): FeatureTransformer = {

    // Tokenisation
    import sqlContext.implicits._

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
      .setOutputCol("vector")
      .setHandleInvalid("skip")

    val model = indexer.fit(docsDF)
    new FeatureTransformer(model)
  }

  def load(path: String): FeatureTransformer = {
    val indexer = StringIndexerModel.load(path + "/indexer")
    new FeatureTransformer(indexer)
  }

}

class FeatureTransformer(indexer: StringIndexerModel) {

  def vocabSize() = {
    indexer.labels.size
  }

  def transform(dataframe: DataFrame): DataFrame = {
    indexer.transform(dataframe)
  }

  def save(path: String): Unit = {
    indexer.save(path + "/indexer")
  }

}
