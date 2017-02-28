package com.sony.prometheus

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Token
import se.lth.cs.docforia.query.QueryCollectors

import scala.collection.JavaConverters._


/**
  * Created by axel on 2017-02-20.
  */

class FeatureExtractorStage(
   path: String,
   featureTransformer: Data,
   trainingDataExtractor: Data)
   (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val trainingSentences = TrainingDataExtractor.load(trainingDataExtractor.getData())
    val ft = FeatureTransformer.load(featureTransformer.getData())
    val data = FeatureExtractor.extract(ft, trainingSentences)
    FeatureExtractor.save(data, path)
  }
}

object FeatureExtractor {
  val NBR_WORDS_BEFORE = 1
  val NBR_WORDS_AFTER = 1

  def extract(ft: FeatureTransformer, trainingSentences: RDD[TrainingSentence])(implicit sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    var df = trainingSentences.zipWithIndex()
      .flatMap(t => featureArray(t._2, t._1))
      .toDF("idx", "rel_id", "rel_name", "relation_idx", "tokens")

    df = ft.transform(df)
    df.show()

    df = df.rdd.map(row => (row.getLong(0), (row.getString(1), row.getString(2), row.getInt(3), row.getDouble(5))))
      .groupByKey().map{
        case (trainingSentence:Long, rows:Iterable[(String, String, Int, Double)]) =>
          val featureList:Seq[Double] = rows.map(_._4).toSeq
          (rows.head._1, rows.head._2, rows.head._3, featureList)
    }.toDF()
    df.show()
    df
  }

  private def featureArray(indx: Long, trainingSentence: TrainingSentence) = {
    val doc = trainingSentence.sentenceDoc
    val NED = NamedEntityDisambiguation.`var`()
    val T = Token.`var`()

    doc.nodes(classOf[Token])
      .asScala
      .toSeq
      .zipWithIndex
      .foreach(t => t._1.putTag("idx", t._2))

    val features = doc.select(NED, T)
      .where(T)
      .coveredBy(NED)
      .stream()
      .collect(QueryCollectors.groupBy(doc, NED).values(T).collector())
      .asScala
      .filter(pg => {
        val qid = pg.key(NED).getIdentifier.split(":").last
        qid == trainingSentence.entityPair.source || qid == trainingSentence.entityPair.source
      })
      .flatMap(grp => {
        val start = grp.value(0, T).getTag("idx"): Int
        val end = grp.value(grp.size() - 1, T).getTag("idx"): Int

        // extract features
        val wordsBefore = doc.nodes(classOf[Token]).asScala.toSeq.slice(start - NBR_WORDS_BEFORE, start)
        val wordsAfter = doc.nodes(classOf[Token]).asScala.toSeq.slice(end + NBR_WORDS_AFTER, end + NBR_WORDS_AFTER + 1)
        // TODO: source or dest wordsBefore
        Seq(wordsBefore.map(_.text()), wordsAfter.map(_.text()))
      }).flatten.filter(Filters.wordFilter)

    features.map(f => {
      (indx, trainingSentence.relationId, trainingSentence.relationName, trainingSentence.relationClass, f)
    })

  }

  def save(data: DataFrame, path: String)(implicit sqlContext: SQLContext): Unit = {
    data.toDF().write.json(path)
  }

  def load(path: String)(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read.json(path)
  }

}
