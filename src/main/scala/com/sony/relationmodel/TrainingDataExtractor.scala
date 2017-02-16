package com.sony.relationmodel

import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Sentence
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.QueryCollectors

/**
  * Created by erik on 2017-02-15.
  */
object TrainingDataExtractor {

  val SENTENCE_MAX_LENGTH = 500
  val SENTENCE_MIN_LENGTH = 5

  def extract(docs: RDD[Document], relations: Array[Relation]): Array[(Relation, RDD[TrainingSentence])] = {

    val trainingData: Array[(Relation, RDD[TrainingSentence])] = relations.map(relation => {
      val data: RDD[TrainingSentence] = docs.flatMap(doc => {

        val S = Sentence.`var`()
        val NED = NamedEntityDisambiguation.`var`()

        val trainingSentences:Seq[TrainingSentence] = doc.select(S, NED)
          .where(NED)
          .coveredBy(S)
          .stream()
          .collect(QueryCollectors.groupBy(doc, S).values(NED).collector()).asScala
          .filter(pg => SENTENCE_MIN_LENGTH <= pg.key(S).length() && pg.key(S).length() <= SENTENCE_MAX_LENGTH)
          .flatMap(pg => {
            val neds: Set[String] = pg.values().asScala.map(_.get(NED).getIdentifier.split(":").last).toSet
            relation.entities.filter(p => neds.contains(p.source) && neds.contains(p.dest))
              .map(p => {
                if(doc.id() == null) {
                  // This is a work around for a bug in Docforia.
                  doc.setId("<null_id>")
                }
                val s = pg.key(S)
                TrainingSentence(doc.subDocument(s.getStart, s.getEnd), p)
              })
          })

        trainingSentences
      })

      Tuple2(relation, data)
    })

    trainingData
  }

  def load(path: String, sqlContext: SQLContext): Array[(Relation, RDD[TrainingSentence])] = {

    import sqlContext.implicits._
    val rawData = sqlContext.read.parquet(path).as[SerializedTrainingSentence].rdd
    val a = rawData.map(st => {
      (st.relation, TrainingSentence(MemoryDocumentIO.getInstance().fromBytes(st.sentenceDoc), st.entityPair))
    }).groupByKey()

  }

  def save(data: Array[(Relation, RDD[TrainingSentence])], path: String, sqlContext: SQLContext): Unit = {

    import sqlContext.implicits._
    data.map(r => r._2.map(t => SerializedTrainingSentence(r._1, t.sentenceDoc.toBytes(), t.entityPair))).reduce((x, y) =>{
      x ++ y
    }).toDF().write.parquet(path)

  }

}

case class TrainingSentence(sentenceDoc: Document, entityPair: EntityPair)
private case class SerializedTrainingSentence(relation: Relation, sentenceDoc: Array[Byte], entityPair: EntityPair)
