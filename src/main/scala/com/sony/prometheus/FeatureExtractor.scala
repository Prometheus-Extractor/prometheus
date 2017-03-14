package com.sony.prometheus

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{SparseVector, Vector, Vectors}
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
        if(f.wordFeatures.length >= MIN_FEATURE_LENGTH) {
          val wordFeats = ft.transformWords(f.wordFeatures).map(_.toDouble).filter(_ >= 0)
          val posFeats = ft.transformPos(f.posFeatures).map(_.toDouble)
          if(neds.contains(f.subj) && neds.contains(f.obj)) {
            Seq(TrainingDataPoint(t.relationId, t.relationName, t.relationClass, wordFeats, posFeats))
          }else {
            Seq(TrainingDataPoint("neg", "neg", 0, wordFeats, posFeats))
          }
        }else{
          Seq()
        }
      })
      featureArrays
    })

    trainingPoints.repartition(Prometheus.DATA_PARTITIONS)
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
        TestDataPoint(sentence, f.subj, f.obj,
          ft.transformWords(f.wordFeatures).map(_.toDouble).filter(_ >= 0),
          ft.transformPos(f.posFeatures).map(_.toDouble))
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
        Find the positions of the entities
         */
        val grp1 :: grp2 :: _ = set.toList
        val start1 = grp1.value(0, T).getTag("idx"): Int
        val end1 = grp1.value(grp1.size() - 1, T).getTag("idx"): Int
        val start2 = grp1.value(0, T).getTag("idx"): Int
        val end2 = grp1.value(grp1.size() - 1, T).getTag("idx"): Int

        val words = wordFeatures(sentence, start1, end1, start2, end2)
        val pos = posFeatures(sentence, start1, end1, start2, end2)

        FeatureArray(sentence,
                     grp1.key(NED).getIdentifier.split(":").last,
                     grp2.key(NED).getIdentifier.split(":").last,
                     words,
                     pos)
      })

    features.toSeq

  }

  /** Extracts a all words features as a list of strings
    */
  private def wordFeatures(sentence: Document, start1: Int, end1: Int, start2: Int, end2: Int): Seq[String] = {
    /*
      Extract words before and after entity 1
     */
    val wordsBefore1 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(start1 - NBR_WORDS_BEFORE, start1)
    val wordsAfter1 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(end1 + NBR_WORDS_AFTER, end1 + NBR_WORDS_AFTER + 1)

    /*
      Extract words before and after entity 2
     */
    val wordsBefore2 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(start2 - NBR_WORDS_BEFORE, start2)
    val wordsAfter2 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(end2 + NBR_WORDS_AFTER, end2 + NBR_WORDS_AFTER + 1)

    /*
      Create string feature vector for the pair
     */
    val features = Seq(
      wordsBefore1.map(_.text()), wordsAfter1.map(_.text()), wordsBefore2.map(_.text()), wordsAfter2.map(_.text())
    ).flatten.filter(Filters.wordFilter)
    features
  }

  private def posFeatures(sentence: Document, start1: Int, end1: Int, start2: Int, end2: Int): Seq[String] = {
    /*
      POS around entity 1 including for entity 1
     */
    val pos1 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(start1 - NBR_WORDS_BEFORE, end1 + NBR_WORDS_AFTER + 1)
    /*
      POS around entity 2 including for entity 2
     */
    val pos2 = sentence.nodes(classOf[Token]).asScala.toSeq.slice(start2 - NBR_WORDS_BEFORE, end2 + NBR_WORDS_AFTER + 1)

    /*
      Create string feature vector for the pair
     */
    val features = Seq(
      pos1.map(_.getPartOfSpeech), pos2.map(_.getPartOfSpeech)
    ).flatten
    features
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

abstract class DataPoint(wordFeature: Seq[Double], posFeature: Seq[Double]) {

  /** Creates a unified vector with [one-hot bag of words, one-hot pos1, one-hot pos2> ...]
   */
  def toFeatureVector(featureTransformer: FeatureTransformer):Vector = {
    val vocabSize = featureTransformer.wordEncoder.vocabSize() + posFeature.length * featureTransformer.posEncoder.vocabSize()
    val indexes: Seq[Double] = wordFeature ++ posFeature.zipWithIndex.map(p => wordFeature.length + p._2 * posFeature.length + p._1)
    oneHotEncode(indexes, vocabSize)
  }

  def oneHotEncode(features: Seq[Double], vocabSize: Int): Vector = {
    val f = features.distinct.map(idx => (idx.toInt, 1.0))
    Vectors.sparse(vocabSize, f)
  }

}
case class TrainingDataPoint(relationId: String, relationName: String, relationClass: Long, wordFeatures: Seq[Double], posFeatures: Seq[Double]) extends DataPoint(wordFeatures, posFeatures)
case class TestDataPoint(sentence: Document, qidSource: String ,qidDest: String, wordFeatures: Seq[Double], posFeatures: Seq[Double]) extends DataPoint(wordFeatures, posFeatures)
case class FeatureArray(sentence: Document, subj: String, obj: String, wordFeatures: Seq[String], posFeatures: Seq[String])
