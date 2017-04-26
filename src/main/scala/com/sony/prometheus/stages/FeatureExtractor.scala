package com.sony.prometheus.stages

import com.sony.prometheus._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.{DependencyRelation, NamedEntity, Token}
import se.lth.cs.docforia.query.QueryCollectors
import com.sony.prometheus.utils.Utils.pathExists

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Stage to extract features, will not run if output already exists
 */
class FeatureExtractorStage(
   path: String,
   trainingDataExtractor: TrainingDataExtractorStage)
   (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
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
      val featureArrays = featureArray(t.sentenceDoc).flatMap(f => {
          val positiveExample = t.entityPair.exists(p => {
            p.dest == f.subj && p.source == f.obj || p.source == f.subj && p.dest == f.obj
          })
          if(positiveExample) {
            Seq(TrainingDataPoint(
              t.relationId,
              t.relationName,
              t.relationClass,
              f.wordFeatures,
              f.posFeatures,
              f.ent1PosFeatures,
              f.ent2PosFeatures,
              f.ent1Type,
              f.ent2Type,
              f.dependencyPath))
          }else {
            Seq(TrainingDataPoint("neg", "neg", 0, f.wordFeatures, f.posFeatures, f.ent1PosFeatures, f.ent2PosFeatures,
              f.ent1Type, f.ent2Type, f.dependencyPath))
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
        TestDataPoint(sentence, f.subj, f.obj, f.wordFeatures, f.posFeatures, f.ent1PosFeatures, f.ent2PosFeatures,
          f.ent1Type, f.ent2Type, f.dependencyPath)
      })
    })

    testPoints
  }

  private def featureArray(sentence: Document): Seq[FeatureArray] = {

    val NED = NamedEntityDisambiguation.`var`()
    val NE = NamedEntity.`var`()
    val T = Token.`var`()

    chunkEntities(sentence)

    sentence.nodes(classOf[Token])
      .asScala
      .toSeq
      .zipWithIndex
      .foreach(t => t._1.putTag("idx", t._2))

    val features = sentence.select(NED, T, NE)
      .where(T)
      .coveredBy(NED)
      .where(T)
      .coveredBy(NE)
      .stream()
      .collect(QueryCollectors.groupBy(sentence, NED, NE).values(T).collector())
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

        /* Windows of words and POS */
        val words = tokenWindow(sentence, start1, end1, start2, end2, t => t.text)
        val pos = tokenWindow(sentence, start1, end1, start2, end2, t => t.getPartOfSpeech)

        /* Entity POS */
        val ent1TokensPos = grp1.nodes[Token](T).asScala.toSeq.map(t => t.getPartOfSpeech).toArray
        val ent2TokensPos = grp2.nodes[Token](T).asScala.toSeq.map(t => t.getPartOfSpeech).toArray

        /* Entity Mention Type */
        val ent1Type = if (grp1.key(NE).hasLabel) grp1.key(NE).getLabel else "<missing label>"
        val ent2Type = if (grp2.key(NE).hasLabel) grp2.key(NE).getLabel else "<missing label>"

        /* Dependecy Path */
        val lastTokenFirstEntity = grp1.value(grp1.size() - 1, T)
        val firstTokenLastEntity =  grp2.value(0, T)
        val dependencyPath = findDependencyPath(lastTokenFirstEntity, Set(), Seq(), firstTokenLastEntity).map(d => {
          DependencyPath(d.getRelation, d.getHead[Token].text, d.getHead[Token].getStart < d.getTail[Token].getStart)
        })

        FeatureArray(
          sentence,
          grp1.key(NED).getIdentifier.split(":").last,
          grp2.key(NED).getIdentifier.split(":").last,
          words,
          pos,
          ent1TokensPos,
          ent2TokensPos,
          ent1Type,
          ent2Type,
          dependencyPath.slice(1, dependencyPath.size - 1))
      }).toSeq

    features

  }

  private def chunkEntities(doc: Document): Document = {
    val NED = NamedEntityDisambiguation.`var`()
    val T = Token.`var`()
    val nedGroups = doc.select(NED, T).where(T).coveredBy(NED)
      .stream()
      .collect(QueryCollectors.groupBy(doc, NED).values(T).collector())
      .asScala
      .toList

    nedGroups.map(pg => {
      pg.key(NED).getIdentifier
      pg.value(0, T).text
      val values = pg.nodes(T).asScala
      if(values.size > 1){
        val head = values.head
        val last = values.last
        head.setRange(head.getStart, last.getEnd)
        values.tail.foreach(doc.remove)
      }
    })
    doc
  }

  /** Extract string features from a Token window around two entities.
    */
  private def tokenWindow(sentence: Document, start1: Int, end1: Int, start2: Int, end2: Int, f: Token => String): Seq[String] = {
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
      Seq.fill(NBR_WORDS_BEFORE - wordsBefore1.length)(EMPTY_TOKEN) ++ wordsBefore1.map(f),
      wordsAfter1.map(f) ++ Seq.fill(NBR_WORDS_AFTER - wordsAfter1.length)(EMPTY_TOKEN),
      Seq.fill(NBR_WORDS_BEFORE - wordsBefore2.length)(EMPTY_TOKEN) ++ wordsBefore2.map(f),
      wordsAfter2.map(f) ++ Seq.fill(NBR_WORDS_AFTER - wordsAfter2.length)(EMPTY_TOKEN)
    ).flatten
    features
  }

  /** Find the Dependency Path between two tokens using DFS.
    */
  def findDependencyPath(current: Token, visited: Set[Token], chain: Seq[DependencyRelation], target: Token): Seq[DependencyRelation] = {
    if(current == target) {
      chain
    }else if(visited.contains(current)){
      Seq()
    }else{
      val deps = current.connectedEdges(classOf[DependencyRelation]).toList.asScala
      val newVisited = visited + current
      deps.flatMap(d => {
        findDependencyPath(d.getHead[Token], newVisited, chain :+ d, target) ++
          findDependencyPath(d.getTail[Token], newVisited, chain :+ d, target)
      })
    }
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

/**
  * Contains a single dependency. Uses java.lang.Boolean for serializing
  */
case class DependencyPath(dependency: String, word: String, direction: java.lang.Boolean)

case class TrainingDataPoint(
  relationId: String,
  relationName: String,
  relationClass: Long,
  wordFeatures: Seq[String],
  posFeatures: Seq[String],
  ent1PosTags: Seq[String],
  ent2PosTags: Seq[String],
  ent1Type: String,
  ent2Type: String,
  dependencyPath: Seq[DependencyPath])

case class TestDataPoint(
  sentence: Document,
  qidSource: String,
  qidDest: String,
  wordFeatures: Seq[String],
  posFeatures: Seq[String],
  ent1PosFeatures: Seq[String],
  ent2PosFeatures: Seq[String],
  ent1Type: String,
  ent2Type: String,
  dependencyPath: Seq[DependencyPath])

case class FeatureArray(
  sentence: Document,
  subj: String,
  obj: String,
  wordFeatures: Seq[String],
  posFeatures: Seq[String],
  ent1PosFeatures: Seq[String],
  ent2PosFeatures: Seq[String],
  ent1Type: String,
  ent2Type: String,
  dependencyPath: Seq[DependencyPath])
