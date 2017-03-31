package com.sony.prometheus.stages

import com.sony.prometheus._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Token
import se.lth.cs.docforia.query.QueryCollectors

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Stage to extract features, will not run if output already exists
 */
class FeatureExtractorStage(
   path: String,
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
    val data = FeatureExtractor.trainingData(trainingSentences)
    FeatureExtractor.save(data, path)
  }
}

/** Extracts features for training/prediction
 */
object FeatureExtractor {

  val EMPTY_TOKEN = "<empty>"
  val NBR_WORDS_BEFORE = 3
  val NBR_WORDS_AFTER = 3
  val MIN_FEATURE_LENGTH = 2

  /** Returns an RDD of [[TrainingDataPoint]]
    *
    * Use this to collect training data for [[RelationModel]]
    *
    * @param trainingSentences  - an RDD of [[TrainingSentence]]
    */
  def trainingData(trainingSentences: RDD[TrainingSentence])(implicit sqlContext: SQLContext): RDD[TrainingDataPoint] = {

    val trainingPoints = trainingSentences.flatMap(t => {
      val neds = new mutable.HashSet() ++ t.entityPair.flatMap(p => Seq(p.source, p.dest))
      val featureArrays = featureArray(t.sentenceDoc).flatMap(f => {
        if(f.wordFeatures.length >= MIN_FEATURE_LENGTH) {
          if(neds.contains(f.subj) && neds.contains(f.obj)) {
            Seq(TrainingDataPoint(t.relationId, t.relationName, t.relationClass, f.wordFeatures, f.posFeatures))
          }else {
            Seq(TrainingDataPoint("neg", "neg", 0, f.wordFeatures, f.posFeatures))
          }
        }else{
          Seq()
        }
      })
      featureArrays
    })

    trainingPoints.repartition(Prometheus.DATA_PARTITIONS)
  }

  /** Returns an RDD of [[TestDataPoint]]
    *
    *   Use this to collect test data for [[RelationModel]]
    *
    *   @param sentences  - a Seq of Docforia Documents
    */
  def testData(sentences: Seq[Document])(implicit sqlContext: SQLContext): Seq[TestDataPoint] = {

    val testPoints = sentences.flatMap(sentence => {
      featureArray(sentence).map(f => {
        TestDataPoint(sentence, f.subj, f.obj, f.wordFeatures, f.posFeatures)
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
        val start2 = grp2.value(0, T).getTag("idx"): Int
        val end2 = grp2.value(grp2.size() - 1, T).getTag("idx"): Int

        val words = tokenWindow(sentence, start1, end1, start2, end2, t => t.text)
        val pos = tokenWindow(sentence, start1, end1, start2, end2, t => t.getPartOfSpeech)

        FeatureArray(sentence,
                     grp1.key(NED).getIdentifier.split(":").last,
                     grp2.key(NED).getIdentifier.split(":").last,
                     words,
                     pos)
      }).toSeq

    features

  }

  /** Extract string features from a Token window around two entities.
    */
  private def tokenWindow(sentence: Document, start1: Int, end1: Int, start2: Int, end2: Int, f: Token => String): Seq[String] = {
    /*
      POS around entity 1 including for entity 1
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
      Seq.fill(NBR_WORDS_BEFORE - wordsBefore1.length)(EMPTY_TOKEN) ++ wordsBefore1.map(f),
      wordsAfter1.map(f) ++ Seq.fill(NBR_WORDS_AFTER - wordsAfter1.length)(EMPTY_TOKEN),
      Seq.fill(NBR_WORDS_BEFORE - wordsBefore2.length)(EMPTY_TOKEN) ++ wordsBefore2.map(f),
      wordsAfter2.map(f) ++ Seq.fill(NBR_WORDS_AFTER - wordsAfter2.length)(EMPTY_TOKEN)
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


case class TrainingDataPoint(relationId: String, relationName: String, relationClass: Long, wordFeatures: Seq[String], posFeatures: Seq[String])
case class TestDataPoint(sentence: Document, qidSource: String ,qidDest: String, wordFeatures: Seq[String], posFeatures: Seq[String])
case class FeatureArray(sentence: Document, subj: String, obj: String, wordFeatures: Seq[String], posFeatures: Seq[String])
