package com.sony.prometheus

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Token
import se.lth.cs.docforia.query.QueryCollectors

import scala.collection.JavaConverters._
import scala.collection.mutable


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
    val data = FeatureExtractor.trainingData(ft, trainingSentences)
    FeatureExtractor.save(data, path)
  }
}

object FeatureExtractor {
  val NBR_WORDS_BEFORE = 1
  val NBR_WORDS_AFTER = 1

  def trainingData(ft: FeatureTransformer, trainingSentences: RDD[TrainingSentence])(implicit sqlContext: SQLContext): RDD[TrainingDataPoint] = {

    val trainingPoints = trainingSentences.map(t => {
      val f = featureArray(t.sentenceDoc, t.entityPair.source, t.entityPair.dest)
      val encodedFeatures = ft.transform(f).map(_.toDouble)

      TrainingDataPoint(t.relationName, t.relationId, t.relationClass, encodedFeatures)
    })

    trainingPoints
  }

  def testData(ft: FeatureTransformer, sentences: RDD[Document])(implicit sqlContext: SQLContext): RDD[TestDataPoint] = {

    val testPoints = sentences.flatMap(sentence => {

      val neds = new mutable.HashSet() ++ sentence.nodes(classOf[NamedEntityDisambiguation]).asScala.toSeq
      val pairs = neds.subsets(2).map(_.toList)
      val features = pairs.flatMap(p => {
        val qid1 = p(0).getIdentifier.split(":").last
        val qid2 = p(1).getIdentifier.split(":").last
        val f1 = featureArray(sentence, qid1, qid2)
        val f2 = featureArray(sentence, qid2, qid1)

        Seq(
          TestDataPoint(sentence, qid1, qid2, ft.transform(f1).map(_.toDouble)),
          TestDataPoint(sentence, qid2, qid1, ft.transform(f2).map(_.toDouble))
        )
      })

      features
    })

    testPoints
  }

  private def featureArray(sentence: Document, sourceQID: String, destQID: String):Seq[String] = {

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

  def save(data: RDD[TrainingDataPoint], path: String)(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    data.toDF().write.json(path)
  }

  def load(path: String)(implicit sqlContext: SQLContext): RDD[TrainingDataPoint]  = {
    import sqlContext.implicits._
    sqlContext.read.json(path).as[TrainingDataPoint].rdd
  }

}

case class TrainingDataPoint(relationId: String, relationName: String, relationClass: Long, features: Seq[Double])
case class TestDataPoint(sentence: Document, qidSource: String ,qidDest: String, features: Seq[Double])
