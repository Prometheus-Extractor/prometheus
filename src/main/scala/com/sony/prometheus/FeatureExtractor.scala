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
import pipeline._

/**
 * Stage to extract features, will not run if output already exists
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

/** Extracts features for training/prediction
 */
object FeatureExtractor {
  val NBR_WORDS_BEFORE = 1
  val NBR_WORDS_AFTER = 1
  val MIN_FEATURE_LENGTH = 2

  /** Returns an RDD of [[com.sony.prometheus.TrainingDataPoint]]
    *
    * Use this to collect training data for [[com.sony.prometheus.RelationModel]]
    *
    * @param ft                 - a [[com.sony.prometheus.FeatureTransformer]]
    * @param trainingSentences  - an RDD of [[com.sony.prometheus.TrainingSentence]]
    */
  def trainingData(ft: FeatureTransformer, trainingSentences: RDD[TrainingSentence])(implicit sqlContext: SQLContext): RDD[TrainingDataPoint] = {

    val trainingPoints = trainingSentences.flatMap(t => {
      val neds = new mutable.HashSet() ++ t.entityPair.flatMap(p => Seq(p.source, p.dest))
      val featureArrays = featureArray(t.sentenceDoc).flatMap(f => {
        if(f.features.length >= MIN_FEATURE_LENGTH) {
          val feats = ft.transform(f.features).map(_.toDouble).filter(_ >= 0)
          if(neds.contains(f.subj) && neds.contains(f.obj)) {
            Seq(TrainingDataPoint(t.relationId, t.relationName, t.relationClass, feats))
          }else {
            Seq(TrainingDataPoint("neg", "neg", 0, feats))
          }
        }else{
          Seq()
        }
      })
      featureArrays
    })

    trainingPoints
  }

  /** Returns an RDD of [[com.sony.prometheus.TestDataPoint]]
    *
    *   Use this to collect test data for [[com.sony.prometheus.RelationModel]]
    *
    *   @param ft     - a [[com.sony.prometheus.FeatureTransformer]]
    *   @param sentences  - a Seq of Docforia Documents
    */
  def testData(ft: FeatureTransformer, sentences: Seq[Document])(implicit sqlContext: SQLContext): Seq[TestDataPoint] = {

    val testPoints = sentences.flatMap(sentence => {
      featureArray(sentence).map(f => {
        TestDataPoint(sentence, f.subj, f.obj, ft.transform(f.features).map(_.toDouble).filter(_ >= 0))
      })
    })

    testPoints
  }

  private def featureArray(sentence: Document):Seq[FeatureArray] = {

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
      .toSet
      .subsets(2)
      .map(set => {
        /*
        Here we extract features for a pair of entities.
         */
        val grp1 :: grp2 :: _ = set.toList

        /*
        Extract words before and after entity 1
         */
        val start1 = grp1.value(0, T).getTag("idx"): Int
        val end1 = grp1.value(grp1.size() - 1, T).getTag("idx"): Int

        val wordsBefore1 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(start1 - NBR_WORDS_BEFORE, start1)
        val wordsAfter1 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(end1 + NBR_WORDS_AFTER, end1 + NBR_WORDS_AFTER + 1)

        /*
        Extract words before and after entity 2
         */
        val start2 = grp1.value(0, T).getTag("idx"): Int
        val end2 = grp1.value(grp1.size() - 1, T).getTag("idx"): Int

        val wordsBefore2 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(start2 - NBR_WORDS_BEFORE, start2)
        val wordsAfter2 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(end2 + NBR_WORDS_AFTER, end2 + NBR_WORDS_AFTER + 1)

        /*
        Create string feature vector for the pair
         */
        val stringFeatures = Seq(
          wordsBefore1.map(_.text()), wordsAfter1.map(_.text()), wordsBefore2.map(_.text()), wordsAfter2.map(_.text())
        ).flatten.filter(Filters.wordFilter)
        FeatureArray(sentence, grp1.key(NED).getIdentifier.split(":").last, grp2.key(NED).getIdentifier.split(":").last, stringFeatures)
      })

    features.toSeq

  }

  /** Saves the training data to the path
   */
  def save(data: RDD[TrainingDataPoint], path: String)(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    data.toDF().write.json(path)
  }

  /** Loads the data from path
   */
  def load(path: String)(implicit sqlContext: SQLContext): RDD[TrainingDataPoint]  = {
    import sqlContext.implicits._
    sqlContext.read.json(path).as[TrainingDataPoint].rdd
  }

}

case class TrainingDataPoint(relationId: String, relationName: String, relationClass: Long, features: Seq[Double])
case class TestDataPoint(sentence: Document, qidSource: String ,qidDest: String, features: Seq[Double])
case class FeatureArray(sentence: Document, subj: String, obj: String, features: Seq[String])
