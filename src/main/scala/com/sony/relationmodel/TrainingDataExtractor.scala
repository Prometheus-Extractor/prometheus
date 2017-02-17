package com.sony.relationmodel

import breeze.linalg.Broadcaster
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Sentence
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.QueryCollectors

import scala.collection.mutable

/**
  * Created by erik on 2017-02-15.
  */
object TrainingDataExtractor {

  val SENTENCE_MAX_LENGTH = 500
  val SENTENCE_MIN_LENGTH = 5
  val PARTIONS = 432

  def extract(docs: RDD[Document], relations: RDD[Relation])(implicit sparkContext: SparkContext): RDD[TrainingSentence] = {

    val broadcastedRelations = relations.sparkContext.broadcast(relations.collect())

    val data:RDD[TrainingSentence] = docs.flatMap(doc => {

        val S = Sentence.`var`()
        val NED = NamedEntityDisambiguation.`var`()

        val trainingSentences:Seq[TrainingSentence] = doc.select(S, NED)
          .where(NED)
          .coveredBy(S)
          .stream()
          .collect(QueryCollectors.groupBy(doc, S).values(NED).collector()).asScala
          .filter(pg => SENTENCE_MIN_LENGTH <= pg.key(S).length() && pg.key(S).length() <= SENTENCE_MAX_LENGTH)
          .flatMap(pg => {

            val trainingData = broadcastedRelations.value.flatMap(relation => {

              val neds = new mutable.HashSet() ++ pg.list(NED).asScala.map(_.getIdentifier.split(":").last)
              relation.entities.toStream.filter(p => neds.contains(p.source) && neds.contains(p.dest))
                .map(p => {
                  if (doc.id() == null) {
                    // This is a work around for a bug in Docforia.
                    doc.setId("<null_id>")
                  }
                  val s = pg.key(S)
                  TrainingSentence(relation.id, relation.name, doc.subDocument(s.getStart, s.getEnd), p)
                })
            })

            trainingData
          })

        trainingSentences

      })

    data

  }

  def load(path: String)(implicit sqlContext: SQLContext): RDD[TrainingSentence] = {

    import sqlContext.implicits._
    val rawData = sqlContext.read.parquet(path).as[SerializedTrainingSentence].rdd
    rawData.map(st => {
      TrainingSentence(st.relationId, st.relationName, MemoryDocumentIO.getInstance().fromBytes(st.sentenceDoc),
                       st.entityPair)
    })

  }

  def save(data: RDD[TrainingSentence], path: String)(implicit sqlContext: SQLContext): Unit = {

    import sqlContext.implicits._
    val serializable = data.map(ts => {
      SerializedTrainingSentence(ts.relationId, ts.relationName, ts.sentenceDoc.toBytes(), ts.entityPair)
    }).toDF()
    serializable.write.parquet(path)
  }

}

case class TrainingSentence(relationId: String, relationName: String, sentenceDoc: Document, entityPair: EntityPair)
private case class SerializedTrainingSentence(relationId: String, relationName: String, sentenceDoc: Array[Byte], entityPair: EntityPair)
