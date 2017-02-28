package com.sony.prometheus

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import se.lth.cs.docforia.Document
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
    val data = FeatureExtractor.extractTrainingData(ft, trainingSentences)
    FeatureExtractor.save(data, path)
  }
}

object FeatureExtractor {
  val NBR_WORDS_BEFORE = 1
  val NBR_WORDS_AFTER = 1

  def extractTrainingData(ft: FeatureTransformer, trainingSentences: RDD[TrainingSentence])(implicit sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._
    var df = trainingSentences.zipWithIndex()
      .flatMap{
        case(t, idx) =>
          featureArray(t.sentenceDoc, t.entityPair.source, t.entityPair.dest).map(f => {
            (idx, t.relationId, t.relationName, t.relationClass, f)
          })

      }.toDF("idx", "rel_id", "rel_name", "relation_idx", "tokens")

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

  private def featureArray(sentence: Document, sourceQID: String, destQID: String) = {

    val NED = NamedEntityDisambiguation.`var`()
    val T = Token.`var`()

    sentence.nodes(classOf[Token])
      .asScala
      .toSeq
      .zipWithIndex
      .foreach(t => t._1.putTag("idx", t._2))

    val features = sentence.select(NED, T)
      .where(T)
      .coveredBy(NED)
      .stream()
      .collect(QueryCollectors.groupBy(sentence, NED).values(T).collector())
      .asScala
      .filter(pg => {
        val qid = pg.key(NED).getIdentifier.split(":").last
        qid == sourceQID || qid == destQID
      })
      .flatMap(grp => {
        val start = grp.value(0, T).getTag("idx"): Int
        val end = grp.value(grp.size() - 1, T).getTag("idx"): Int

        // extract features
        val wordsBefore = sentence.nodes(classOf[Token]).asScala.toSeq.slice(start - NBR_WORDS_BEFORE, start)
        val wordsAfter = sentence.nodes(classOf[Token]).asScala.toSeq.slice(end + NBR_WORDS_AFTER, end + NBR_WORDS_AFTER + 1)
        // TODO: source or dest wordsBefore
        Seq(wordsBefore.map(_.text()), wordsAfter.map(_.text()))
      }).flatten.filter(Filters.wordFilter)

    features

  }

  def save(data: DataFrame, path: String)(implicit sqlContext: SQLContext): Unit = {
    data.toDF().write.json(path)
  }

  def load(path: String)(implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read.json(path)
  }

}
