package com.sony.relationmodel

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
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

  override def getData(force: Boolean = false): String = {
    if (!exists(path) || force) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val trainingSentences = TrainingDataExtractor.load(trainingDataExtractor.getData())
    val pipelineModel = PipelineModel.load(featureTransformer.getData())
    FeatureExtractor.extract(pipelineModel, trainingSentences).write.parquet(path)
  }
}

object FeatureExtractor {
  val NBR_WORDS_BEFORE = 1
  val NBR_WORDS_AFTER = 1

  def extract(pipelineModel: PipelineModel, trainingSentences: RDD[TrainingSentence])(implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val df = trainingSentences.map(featureArray).map(Tuple1.apply).toDF("tokens")
    pipelineModel.transform(df)
  }

  private def featureArray(trainingSentence: TrainingSentence): Seq[String] = {
    val doc = trainingSentence.sentenceDoc
    val NED = NamedEntityDisambiguation.`var`()
    val T = Token.`var`()

     doc.nodes(classOf[Token])
      .asScala
      .toSeq
      .zipWithIndex
      .foreach(t => t._1.putTag("idx", t._2))

    doc.select(NED, T)
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
       Array(wordsBefore.map(_.text()), wordsAfter.map(_.text()))
      }).flatten
  }

}
