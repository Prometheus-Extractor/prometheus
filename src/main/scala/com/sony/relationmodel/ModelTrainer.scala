package com.sony.relationmodel

import scala.collection.JavaConverters._
import java.io.IOError
import java.util.regex.Pattern

import org.apache.log4j.LogManager
import org.apache.spark.ml.feature.{HashingTF, OneHotEncoder, StringIndexer, Word2Vec}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Token
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.QueryCollectors

import scala.util.Properties.envOrNone

object ModelTrainer {
  val usage = """
  Usage: ModelTrainer <path/to/corpus> <path/to/relations parquet file>
  """

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(usage)
      sys.exit(1)
    }

    val log = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("Fact Extractor")
    envOrNone("SPARK_MASTER").foreach(m => conf.setMaster(m))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val relations: RDD[RelationRow] = RelationsReader.readRelations(sqlContext, args(1))
    val docs: RDD[Document] = CorpusReader.readCorpus(sqlContext, sc, args(0))

    val wordPattern = Pattern.compile("\\p{L}{2,}|\\d{4}]")

    import sqlContext.implicits._

    // Tokenization
    val T = Token.`var`()
    val docsDF = docs.flatMap(doc => {
      doc.nodes(classOf[Token]).asScala.toList.map(t => t.text())
    }).filter(t => wordPattern.matcher(t).matches())
      .map(token => (token, 1))
      .reduceByKey(_+_)
      .filter(tup => tup._2 >= 3)
      .map(_._1)
      .toDF("tokens")

    val indexer = new StringIndexer()
      .setInputCol("tokens")
      .setOutputCol("categoryIndex")
      .fit(docsDF)
    val indexed = indexer.transform(docsDF)

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
    val encoded = encoder.transform(indexed)
    encoded.select("tokens", "categoryVec").show(10, false)




    sc.stop()
  }

}
