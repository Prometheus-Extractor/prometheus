package com.sony.prometheus.stages

import java.util

import com.sony.prometheus.Prometheus
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import com.sony.prometheus.utils.Utils.pathExists
import org.apache.log4j.LogManager
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.dataset.DataSet

import scala.collection.mutable.ListBuffer
import scala.util.Random

class FeatureTransformerStage(path: String, word2VecData: Word2VecData, posEncoderStage: PosEncoderStage,
                              neTypeEncoder: NeTypeEncoderStage, dependencyEncoderStage: DependencyEncoderStage,
                              featureExtractorStage: FeatureExtractorStage)
                             (implicit sqlContext:SQLContext, sparkContext: SparkContext) extends Task with Data{
  /**
    * Runs the task, saving results to disk
    */
  override def run(): Unit = {

    val data = FeatureExtractor.load(featureExtractorStage.getData())
    val labelNames: util.List[String] = ListBuffer(
      data.map(d => (d.relationClass, d.relationId)).distinct().collect().sortBy(_._1).map(_._2).toList: _*)

    val numClasses = data.map(d => d.relationClass).distinct().count().toInt
    val balancedData = FeatureTransformer.balanceData(data, false)

    val featureTransformer = sqlContext.sparkContext.broadcast(
      FeatureTransformer(word2VecData.getData(), posEncoderStage.getData(), neTypeEncoder.getData(),
                         dependencyEncoderStage.getData()))
    balancedData.map(d => {
      val vector = featureTransformer.value.toFeatureVector(
        d.wordFeatures, d.posFeatures, d.wordsBetween, d.posBetween, d.ent1PosTags, d.ent2PosTags, d.ent1Type, d.ent2Type, d.dependencyPath,
        d.ent1DepWindow, d.ent2DepWindow
      ).toArray.map(_.toFloat)
      val features = Nd4j.create(vector)
      val label = Nd4j.create(featureTransformer.value.oneHotEncode(Seq(d.relationClass.toInt), numClasses).toArray)
      val dataset = new DataSet(features, label)
      dataset.setLabelNames(labelNames)
      dataset
    }).saveAsObjectFile(path)
    featureTransformer.destroy()

  }

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

}

/** Used for creating a FeatureTransformer
 */
object FeatureTransformer {

  val log = LogManager.getLogger(classOf[FeatureTransformer])

  def apply(pathToWord2Vec: String, pathToPosEncoder: String, pathToNeType: String, pathToDepEncoder: String)
           (implicit sqlContext: SQLContext): FeatureTransformer = {
    val posEncoder = StringIndexer.load(pathToPosEncoder, sqlContext.sparkContext)
    val word2vec = Word2VecEncoder.apply(pathToWord2Vec)
    val neType = StringIndexer.load(pathToNeType, sqlContext.sparkContext)
    val depEncoder = StringIndexer.load(pathToDepEncoder, sqlContext.sparkContext)
    new FeatureTransformer(word2vec, posEncoder, neType, depEncoder)
  }

  def load(path: String)(implicit sqlContext: SQLContext): RDD[DataSet] = {
    sqlContext.sparkContext.objectFile[DataSet](path)
  }

  /**
    * Rebalances an imbalanced dataset. Either undersample or oversample.
    * Balances to match biggest or smallest class, excluding class 0, i.e. the negative class.
    */
  def balanceData(rawData: RDD[TrainingDataPoint], underSample: Boolean = false): RDD[TrainingDataPoint] = {

    val classCount = rawData.map(d => d.relationClass).countByValue()
    val posClasses = classCount.filter(_._1 != FeatureExtractor.NEGATIVE_CLASS_NBR)
    val sampleTo = if (underSample) posClasses.map(_._2).min else posClasses.map(_._2).max
    val nbrClasses = classCount.keys.size

    log.info(s"Rebalancing dataset (${if (underSample) "undersample" else "oversample"})")
    classCount.foreach(pair => log.info(s"\tClass ${pair._1}: ${pair._2}"))

    /* Resample positive classes */
    val balancedDataset = classCount.map{
/*      case (FeatureExtractor.NEGATIVE_CLASS_NBR, count: Long) =>
        val samplePercentage = sampleTo / count.toDouble * 0.625
        val replacement = sampleTo > count
        val neg = rawData.filter(d => d.relationClass == FeatureExtractor.NEGATIVE_CLASS_NBR && d.pointType == FeatureExtractor.DATA_TYPE_NEG)
          .sample(replacement, samplePercentage)
        val nearPos = rawData.filter(d => d.relationClass == FeatureExtractor.NEGATIVE_CLASS_NBR && d.pointType == FeatureExtractor.DATA_TYPE_NEARPOS)
            .sample(replacement, samplePercentage)
        neg ++ nearPos
*/
      case (key: Long, count: Long) =>
        /* Make all positive classes equally big and our two negative types ,*/
        val samplePercentage = sampleTo / count.toDouble
        val replacement = sampleTo > count
        rawData.filter(d => d.relationClass == key).sample(replacement, samplePercentage)

    }.reduce(_++_)

    log.info("Balanced result:")
    balancedDataset.map(d => d.relationClass + d.pointType.toString).countByValue().foreach(pair => log.info(s"\tClass ${pair._1}: ${pair._2}"))
    balancedDataset.repartition(Prometheus.DATA_PARTITIONS)
    balancedDataset.mapPartitions(Random.shuffle(_))
  }

}

/**
 */
class FeatureTransformer(wordEncoder: Word2VecEncoder, posEncoder: StringIndexer,
                         neTypeEncoder: StringIndexer, dependencyEncoder: StringIndexer) extends Serializable {

  val DEPENDENCY_FEATURE_SIZE = 8
  val WORDS_BETWEEN_SIZE = 8

  lazy val emptyDependencyVector = oneHotEncode(Seq(0), dependencyEncoder.vocabSize()).toArray ++
                                  wordEncoder.emptyVector.toArray ++
                                  Array(0.0)

  def oneHotEncode(features: Seq[Int], vocabSize: Int): Vector = {
    val f = features.distinct.map(idx => (idx, 1.0))
    Vectors.sparse(vocabSize, f)
  }

  /** Creates a unified vector with all the features
    * @param  wordFeatures  the word features, a Seq of words
    * @param  posFeatures   the part-of-speech tags for the word features
    * @param  ent1TokensPos the part-of-speech tags for entity1's tokens
    * @param  ent2TokensPos the part-of-speech tags for entity2's tokens
    * @return a unified feature vector
    */
  def toFeatureVector(wordFeatures: Seq[String], posFeatures: Seq[String], wordsBetween: Seq[String],
                      posBetween: Seq[String], ent1TokensPos: Seq[String],
                      ent2TokensPos: Seq[String], ent1Type: String, ent2Type: String,
                      dependencyPath: Seq[DependencyPath], ent1DepWindow: Seq[DependencyPath],
                      ent2DepWindow: Seq[DependencyPath]): Vector = {

    /* Word features */
    val wordVectors = wordFeatures.map(wordEncoder.index).flatMap(_.toArray).toArray

    /* Part of speech features */
    val posVectors = posFeatures.map(posEncoder.index).map(Seq(_))
      .map(oneHotEncode(_, posEncoder.vocabSize()).toArray).flatten.toArray

    val ent1Pos = oneHotEncode(    // eg Seq(ADJ, PROPER_NOUN, PROPER_NOUN) repr. (Venerable Barack Obama)
      ent1TokensPos.map(posEncoder.index),  // eg Seq(0, 2, 2) (index of the POS tags)
      posEncoder.vocabSize()
    ).toArray // one-hot encoded, eg Array(1, 0, 1, 0, 0, ... 0) with length posEncoder.vocabSize()

    val ent2Pos = oneHotEncode(
      ent2TokensPos.map(posEncoder.index),
      posEncoder.vocabSize()
    ).toArray

    /* Sequence of words between the two entities */
    val wordsBetweenVectors = wordsBetween.slice(0, WORDS_BETWEEN_SIZE).map(wordEncoder.index).map(_.toArray)
    val wordsPadding = Seq.fill(WORDS_BETWEEN_SIZE - wordsBetween.size)(wordEncoder.emptyVector.toArray)
    val paddedWordsBetweenVectors = (wordsBetweenVectors ++ wordsPadding).flatten.toArray
    // ... and their POS tags
    val posBetweenVectors = posBetween.slice(0, WORDS_BETWEEN_SIZE).map(posEncoder.index).map(Seq(_))
      .map(oneHotEncode(_, posEncoder.vocabSize()).toArray)
    val posPadding = Seq.fill(WORDS_BETWEEN_SIZE - posBetween.size)(oneHotEncode(Seq(0), dependencyEncoder.vocabSize()).toArray)
    val paddedPosVectors = (posBetweenVectors ++ posPadding).flatten.toArray

    /* Named entity types */
    val neType1 = oneHotEncode(Seq(neTypeEncoder.index(ent1Type)), neTypeEncoder.vocabSize()).toArray
    val neType2 = oneHotEncode(Seq(neTypeEncoder.index(ent1Type)), neTypeEncoder.vocabSize()).toArray

    /* Dependency Path */
    val depPath = dependencyPath.map(d => {
      oneHotEncode(Seq(dependencyEncoder.index(d.dependency)), dependencyEncoder.vocabSize()).toArray ++
        wordEncoder.index(d.word).toArray ++
        (if (d.direction) Array(1.0) else Array(0.0))
    })

    val paddedDepPath = (depPath.slice(0, DEPENDENCY_FEATURE_SIZE) ++
      Seq.fill(DEPENDENCY_FEATURE_SIZE - depPath.size)(emptyDependencyVector)).flatten

    /* Dependency windows */
    val ent1PaddedDepWindow = (ent1DepWindow.map(d => {
      oneHotEncode(Seq(dependencyEncoder.index(d.dependency)), dependencyEncoder.vocabSize()).toArray ++
        wordEncoder.index(d.word).toArray ++
        (if (d.direction) Array(1.0) else Array(0.0))
    }) ++ Seq.fill(FeatureExtractor.DEPENDENCY_WINDOW - ent1DepWindow.size)(emptyDependencyVector)).flatten

    val ent2PaddedDepWindow = (ent2DepWindow.map(d => {
      oneHotEncode(Seq(dependencyEncoder.index(d.dependency)), dependencyEncoder.vocabSize()).toArray ++
        wordEncoder.index(d.word).toArray ++
        (if (d.direction) Array(1.0) else Array(0.0))
    }) ++ Seq.fill(FeatureExtractor.DEPENDENCY_WINDOW - ent2DepWindow.size)(emptyDependencyVector)).flatten

    Vectors.dense(wordVectors ++ posVectors ++ paddedWordsBetweenVectors ++ paddedPosVectors ++ ent1Pos ++ ent2Pos
      ++ neType1 ++ neType2 ++ paddedDepPath ++ ent1PaddedDepWindow  ++ ent2PaddedDepWindow)
  }
}
