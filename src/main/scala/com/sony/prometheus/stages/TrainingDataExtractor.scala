package com.sony.prometheus.stages

import com.sony.prometheus._
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Sentence
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.{NodeTVar, PropositionGroup, QueryCollectors}
import com.sony.prometheus.utils.Utils.pathExists

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** Stage for training data extraction
 */
class TrainingDataExtractorStage(
  path: String,
  corpusData: CorpusData,
  entityPairs: EntityPairExtractorStage)
  (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val relations = EntityPairExtractor.load(entityPairs.getData())
    val docs = CorpusReader.readCorpus(corpusData.getData(), corpusData.sampleSize)
    val sentences = TrainingDataExtractor.extract(docs, relations)
    TrainingDataExtractor.save(sentences, path)
    TrainingDataExtractor.printInfo(docs, relations, sentences)
  }
}

/** Provides training data extraction
 */
object TrainingDataExtractor {

  val log = LogManager.getLogger(TrainingDataExtractor.getClass)
  val SENTENCE_MAX_LENGTH = 220
  val SENTENCE_MIN_LENGTH = 5
  val NEGATIVE_CLASS_NAME = "neg"
  val NEGATIVE_CLASS_NBR = 0
  val NEGATIVE_SAMPLING_HEURISTIC = 0.05

  /**
   * Extracts RDD of [[TrainingSentence]]
   */
  def extract(corpus: RDD[Document], relations: RDD[Relation])
             (implicit sparkContext: SparkContext): RDD[TrainingSentence] = {
    val relMapping = relations.map(r => {
      val entPairMap = new Object2ObjectOpenHashMap[String, mutable.Set[String]]()
        r.entities.foreach(ep =>{
          val dests = entPairMap.getOrDefault(ep.source, new mutable.HashSet[String]())
          dests += ep.dest
          entPairMap.put(ep.source, dests)
        })
      (r, entPairMap)
    })

    val broadcastRM = sparkContext.broadcast(relMapping.collect())


    // Runs "extractor" over docs's sentences to produce RDD of TrainingSentence
    def extractExamples(docs: RDD[Document],
                        extractor: (Document, NodeTVar[Sentence], NodeTVar[NamedEntityDisambiguation], PropositionGroup) => Seq[TrainingSentence]): RDD[TrainingSentence] = {
      docs.flatMap(doc => {

        val S = Sentence.`var`()
        val NED = NamedEntityDisambiguation.`var`()

        val sentences: Seq[PropositionGroup] = doc.select(S, NED)
          .where(NED)
          .coveredBy(S)
          .stream()
          .collect(QueryCollectors.groupBy(doc, S).values(NED).collector()).asScala
          .filter(pg => SENTENCE_MIN_LENGTH <= pg.key(S).length() && pg.key(S).length() <= SENTENCE_MAX_LENGTH)

        sentences.flatMap(extractor(doc, S, NED, _))
      })
    }


    // Extracts positive examples
    // Positive examples are those sentences that contain at least one entity pair
    // known (from "relations") to partake in a specific relation (eg Göran Persson, Anitra Steen)
    def positiveExtractor(doc: Document, S: NodeTVar[Sentence], NED: NodeTVar[NamedEntityDisambiguation], pg: PropositionGroup): Seq[TrainingSentence] = {
      lazy val sDoc = doc.subDocument(pg.key(S).getStart, pg.key(S).getEnd)
      val neds = pg.list(NED).asScala.map(_.getIdentifier.split(":").last).toSet.subsets(2).map(_.toSeq).toSeq

      val trainingData = broadcastRM.value.flatMap{
        case (relation, mapping) => {
          val knownPairs = neds.flatMap(pair => {
            val foundPairs = ListBuffer[EntityPair]()
            if (mapping.getOrDefault(pair(0), mutable.Set.empty).contains(pair(1))){
              foundPairs += EntityPair(pair(0), pair(1))
            } else if (mapping.getOrDefault(pair(1), mutable.Set.empty).contains(pair(0))){
              foundPairs += EntityPair(pair(1), pair(0))
            }
            foundPairs
          })

          if (knownPairs.nonEmpty) {
            Seq(TrainingSentence(relation.id, relation.name, relation.classIdx, sDoc, knownPairs.toList, true))
          } else {
            Seq()
          }
        }
      }
      trainingData
    }

    // Extracts negative examples
    def negativeExtractor(doc: Document, S: NodeTVar[Sentence], NED: NodeTVar[NamedEntityDisambiguation], pg: PropositionGroup): Seq[TrainingSentence] = {
      lazy val sDoc = doc.subDocument(pg.key(S).getStart, pg.key(S).getEnd)

      val trainingData = broadcastRM.value.flatMap{
        case (_, mapping) => {
          // Negative examples are random sentences that contain any entity pair not known
          val entityPairs = pg.list(NED).asScala
            .map(_.getIdentifier.split(":").last)
            .toSet.subsets(2).map(_.toSeq)
            .filter(p => {
              // Remove pairs that are known to partake in the relation
              !mapping.getOrDefault(p(0), mutable.Set.empty).contains(p(1)) &&
                !mapping.getOrDefault(p(1), mutable.Set.empty).contains(p(0))
            })
            .map(p => EntityPair(p(0), p(1)))
            .toSeq

          if (entityPairs.nonEmpty) {
            Seq(TrainingSentence(NEGATIVE_CLASS_NAME, NEGATIVE_CLASS_NAME, NEGATIVE_CLASS_NBR, sDoc, entityPairs.toList, false))
          } else {
            Seq()
          }
        }
      }
      trainingData
    }

    val totalDocs = corpus.count()
    log.info(s"there are $totalDocs docs")
    // Extract (true and false) positive examples over the whole corpus
    val positiveExamples = extractExamples(corpus, positiveExtractor)
    val nbrPositive = positiveExamples.count()
    log.info(s"positive examples $nbrPositive")

    // Extract negative examples over a small part of the corpus (otherwise we will find too many true negative examples)
    val negativeSampleHeuristic = nbrPositive.toDouble / totalDocs
    log.info(s"negative sample heuristic: $negativeSampleHeuristic")

    val negativeExamples = extractExamples(corpus.sample(false, negativeSampleHeuristic), negativeExtractor)
    val nbrNegative = negativeExamples.count()
    log.info(s"negative examples $nbrNegative")

    val unioned = negativeExamples ++ positiveExamples
    log.info(s"unionend ${unioned.count()}")

    unioned.repartition(Prometheus.DATA_PARTITIONS)
  }

  def load(path: String)(implicit sqlContext: SQLContext): RDD[TrainingSentence] = {

    import sqlContext.implicits._
    val rawData = sqlContext.read.parquet(path).as[SerializedTrainingSentence].rdd
    rawData.map(st => {
      TrainingSentence(st.relationId, st.relationName, st.relationClass,
                       MemoryDocumentIO.getInstance().fromBytes(st.sentenceDoc), st.entityPair, st.positive)
    })

  }

  def save(data: RDD[TrainingSentence], path: String)(implicit sqlContext: SQLContext): Unit = {
    println(s"Saving training data to $path")
    import sqlContext.implicits._
    val serializable = data.map(ts => {
      SerializedTrainingSentence(
        ts.relationId,
        ts.relationName,
        ts.relationClass,
        ts.sentenceDoc.toBytes(),
        ts.entityPair,
        ts.positive)
    }).toDF()
    serializable.write.parquet(path)
  }

  def printInfo(docs: RDD[Document], relations: RDD[Relation], sentences: RDD[TrainingSentence]): Unit = {
    val log = LogManager.getLogger(TrainingDataExtractor.getClass)
    val docsCount = docs.count()
    val positiveCount = sentences.filter(_.relationClass == NEGATIVE_CLASS_NBR).count()
    val relationsCount = relations.count()
    val entitiesCount = relations.map(r => (r.id -> r.entities.length)).collect().toMap
    //val dist = sentences.map(t => (t.relationId, 1)).reduceByKey(_ + _)
    //  .map(t => s"${t._1}\t-> ${entitiesCount.getOrElse(t._1, 0)} entities\t-> ${t._2}").collect()

    log.info("Extracting Training Sentences")
    log.info(s"Documents: $docsCount")
    log.info(s"Relations: $relationsCount")
    log.info(s"Positive sentences: $positiveCount")
    //dist.map(log.info)
  }
}

case class TrainingSentence(
  relationId: String,
  relationName: String,
  relationClass: Int,
  sentenceDoc: Document,
  entityPair: Seq[EntityPair],
  positive: Boolean)

private case class SerializedTrainingSentence(
  relationId: String,
  relationName: String,
  relationClass: Int,
  sentenceDoc: Array[Byte],
  entityPair: Seq[EntityPair],
  positive: Boolean)
