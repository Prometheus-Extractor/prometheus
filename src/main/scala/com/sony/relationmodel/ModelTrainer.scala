package com.sony.relationmodel

import scala.collection.JavaConverters._
import java.io.IOError
import java.util.regex.Pattern

import org.apache.log4j.LogManager
import org.apache.spark.ml.feature.{HashingTF, OneHotEncoder, StringIndexer, Word2Vec}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.{NamedEntity, Sentence, Token}
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.QueryCollectors
import org.rogach.scallop._
import org.rogach.scallop.exceptions._
import scala.util.Properties.envOrNone
import java.nio.file.{Paths, Files}

object ModelTrainer {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    version("Prometheus Model Trainer 0.0.1-SNAPSHOT")
    banner("""Usage: ModelTrainer [--force] corpus-path relations-path
           |Prometheus model trainer trains a relation extractor
           |Options:
           |""".stripMargin)
    val corpusPath = trailArg[String](descr = "path to the corpus to train on")
    val relationsPath = trailArg[String](descr = "path to a parquet file with the relations")
    val force = opt[Boolean](descr = "set this to invalidate cached intermediate results")
    verify()
    override def onError(e: Throwable) = e match {
      case ScallopException(message) =>
        printHelp
        sys.exit(1)
      case ex => super.onError(ex)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val log = LogManager.getRootLogger
    val sparkConf = new SparkConf().setAppName("Prometheus Relation Model")
    envOrNone("SPARK_MASTER").foreach(m => sparkConf.setMaster(m))

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val relations: Array[Relation] = RelationsReader.readRelations(sqlContext, conf.relationsPath()).collect()
    val docs: RDD[Document] = CorpusReader.readCorpus(sqlContext, sc, conf.corpusPath(), 1.0)

    val trainingData = TrainingDataExtractor.extract(docs, relations)


//    // Tokenization
//    val wordPattern = Pattern.compile("\\p{L}{2,}|\\d{4}]")
//
//    val T = Token.`var`()
//    val docsDF = docs.flatMap(doc => {
//      doc.nodes(classOf[Token]).asScala.toSeq.map(t => t.text())
//    }).filter(t => wordPattern.matcher(t).matches())
//      .map(token => (token, 1))
//      .reduceByKey(_+_)
//      .filter(tup => tup._2 >= 3)
//      .map(_._1)
//      .toDF("tokens")
//
//    val indexer = new StringIndexer()
//      .setInputCol("tokens")
//      .setOutputCol("categoryIndex")
//      .fit(docsDF)
//    val indexed = indexer.transform(docsDF)
//
//    val encoder = new OneHotEncoder()
//      .setInputCol("categoryIndex")
//      .setOutputCol("categoryVec")
//    val encoded = encoder.transform(indexed)



    sc.stop()
  }

}

