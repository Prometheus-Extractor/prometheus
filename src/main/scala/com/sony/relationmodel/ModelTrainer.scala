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

import scala.util.Properties.envOrNone

object ModelTrainer {

  val SENTENCE_MAX_LENGTH = 200
  val USAGE = """
  Usage: ModelTrainer <path/to/corpus> <path/to/relations parquet file>
  """

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(USAGE)
      sys.exit(1)
    }

    val log = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("Prometheus Relation Model")
    envOrNone("SPARK_MASTER").foreach(m => conf.setMaster(m))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val relations: RDD[Relation] = RelationsReader.readRelations(sqlContext, args(1)).rdd
    val docs: RDD[Document] = CorpusReader.readCorpus(sqlContext, sc, args(0), 0.01)



    val wordPattern = Pattern.compile("\\p{L}{2,}|\\d{4}]")

    import sqlContext.implicits._

    // Tokenization
    val T = Token.`var`()
    val docsDF = docs.flatMap(doc => {
      doc.nodes(classOf[Token]).asScala.toSeq.map(t => t.text())
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


    val S = Sentence.`var`()
    val NED = NamedEntityDisambiguation.`var`()

    import sqlContext.implicits._

    val trainingData = relations.flatMap(relation => {
      val data = docs.flatMap(doc => {
        doc.select(S, NED)
          .where(NED)
          .coveredBy(S)
          .stream()
          .collect(QueryCollectors.groupBy(doc, S).values(NED).collector()).asScala
          .filter(_.key(S).length() <= SENTENCE_MAX_LENGTH)
          .flatMap(pg => {
            val neds: Set[String] = pg.values().asScala.map(_.get(NED).getIdentifier.split(":").last).toSet
            relation.entities.filter(p => neds.contains(p.source) && neds.contains(p.dest)).map(p => {
              val s = pg.key(S)
              TrainingSentence(doc.subDocument(s.getStart, s.getEnd), p)
            })
          })
      })

    data.collect().toSeq
    })

    trainingData.toDF().show()

    // för varje relation
    //    träna en model
    //      på datan av meningar som innehåller de entiterna
    //    spara ned modellen till fil.

    sc.stop()
  }

}

case class TrainingSentence(sentenceDoc: Document, entityPair: EntityPair)
