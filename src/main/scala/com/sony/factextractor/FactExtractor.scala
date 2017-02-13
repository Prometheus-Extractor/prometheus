package com.sony.factextractor

import java.io.IOError

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import scala.util.Properties.envOrNone

object FactExtractor {

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("Fact Extractor")
    envOrNone("SPARK_MASTER").foreach(m => conf.setMaster(m))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val relations: RDD[RelationRow] = RelationsReader.readRelations(sqlContext, args(1))
    val docs: RDD[Document] = CorpusReader.readCorpus(sqlContext, sc, args(0))

    // Global feature extraction?



    // Train models
    relations.foreach(relation => {

      val model = RelationModel.train(relation, docs)

    })

    sc.stop()
  }

}
