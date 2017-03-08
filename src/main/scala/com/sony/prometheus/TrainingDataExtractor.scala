package com.sony.prometheus

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Sentence
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.QueryCollectors

import scala.collection.JavaConverters._
import scala.collection.mutable
import pipeline._

/** Stage for training data extraction
 */
class TrainingDataExtractorStage(
  path: String,
  corpusData: Data,
  relationsData: Data)
  (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val relations = RelationsReader.readRelations(relationsData.getData())
    val docs = CorpusReader.readCorpus(corpusData.getData())
    val sentences = TrainingDataExtractor.extract(docs, relations)
    TrainingDataExtractor.save(sentences, path)
  }
}

/** Provides training data extraction
 */
object TrainingDataExtractor {

  val SENTENCE_MAX_LENGTH = 500
  val SENTENCE_MIN_LENGTH = 5
  val PARTIONS = 432

  /**
   * Extracts RDD of [[com.sony.prometheus.TrainingSentence]]
   */
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

            if (doc.id() == null) {
              // This is a work around for a bug in Docforia.
              doc.setId("<null_id>")
            }
            val neds = new mutable.HashSet() ++ pg.list(NED).asScala.map(_.getIdentifier.split(":").last)
            lazy val sDoc = doc.subDocument(pg.key(S).getStart, pg.key(S).getEnd)

            val trainingData = broadcastedRelations.value.flatMap(relation => {
              val pairs = relation.entities.toStream.filter(p => neds.contains(p.source) && neds.contains(p.dest))
              if (pairs.length > 0) {
                Seq(TrainingSentence(relation.id, relation.name, relation.classIdx, sDoc, pairs))
              } else {
                Seq()
              }
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
      TrainingSentence(st.relationId, st.relationName, st.relationClass,
                       MemoryDocumentIO.getInstance().fromBytes(st.sentenceDoc), st.entityPair)
    })

  }

  def save(data: RDD[TrainingSentence], path: String)(implicit sqlContext: SQLContext): Unit = {

    import sqlContext.implicits._
    val serializable = data.map(ts => {
      SerializedTrainingSentence(ts.relationId, ts.relationName, ts.relationClass,
                                 ts.sentenceDoc.toBytes(), ts.entityPair)
    }).toDF()
    serializable.write.parquet(path)
  }

}

case class TrainingSentence(relationId: String, relationName: String, relationClass: Int, sentenceDoc: Document, entityPair: Seq[EntityPair])
private case class SerializedTrainingSentence(relationId: String, relationName: String, relationClass: Int, sentenceDoc: Array[Byte], entityPair: Seq[EntityPair])
