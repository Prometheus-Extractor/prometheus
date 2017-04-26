package com.sony.prometheus.stages

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import play.api.libs.json._
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Sentence

import scala.collection.JavaConverters._
import com.sony.prometheus.utils.Utils.pathExists

/**
  * Created by erik on 2017-02-28.
  */
object Predictor {

  def apply(modelStage: RelationModelStage, posEncoder: PosEncoderStage, word2VecData: Word2VecData,
            neTypeEncoder: NeTypeEncoderStage, dependencyEncoderStage: DependencyEncoderStage, entityPairs: EntityPairExtractorStage)
           (implicit sqlContext: SQLContext): Predictor = {

    val featureTransformer = FeatureTransformer(word2VecData.getData(), posEncoder.getData(), neTypeEncoder.getData(),
                                                dependencyEncoderStage.getData())
    val model = RelationModel.load(modelStage.getData(), sqlContext.sparkContext)
    val relations = EntityPairExtractor.load(entityPairs.getData())
    val ft = sqlContext.sparkContext.broadcast(featureTransformer)
    new Predictor(model, ft, relations)
  }

  def load(path: String)(implicit sqlContext: SQLContext): RDD[ExtractedRelation] = {
    import sqlContext.implicits._
    sqlContext.read.json(path).as[ExtractedRelation].rdd
  }

  def save(data: RDD[Seq[ExtractedRelation]], path: String)(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    data.flatMap(d => d).toDF().write.json(path)
  }

}

class Predictor(model: RelationModel, transformer: Broadcast[FeatureTransformer], relations: RDD[Relation]) extends Serializable {

  val UNKNOWN_CLASS = "<unknown_class>"

  def extractRelations(docs: RDD[Document])(implicit sqlContext: SQLContext): RDD[Seq[ExtractedRelation]] = {

    val classIdxToId: Map[Int, String] = relations.map(r => (r.classIdx, r.id)).collect().toList.toMap

    docs.map(doc => {
      val sentences = doc.nodes(classOf[Sentence])
        .asScala
        .toSeq
        .map(s => doc.subDocument(s.getStart, s.getEnd))

      val points: Seq[TestDataPoint] = FeatureExtractor.testData(sentences)
      val classes = points
        .map(p => transformer.value.toFeatureVector(p.wordFeatures, p.posFeatures, p.ent1PosFeatures, p.ent2PosFeatures,
          p.ent1Type, p.ent2Type, p.dependencyPath))
        .map(model.predict)

      classes.zip(points).map{
        case (result: Prediction, point: TestDataPoint) =>
          val predicate = classIdxToId.getOrElse(result.clsIdx, s"$UNKNOWN_CLASS: $result.clsIdx>")
          ExtractedRelation(point.qidSource, predicate, point.qidDest, point.sentence.text(), doc.uri(), result.probability)
      }.toList
    })
  }

}

case class ExtractedRelation(
  subject: String,
  predictedPredicate: String,
  obj: String,
  sentence: String,
  source: String,
  probability: Double)

object ExtractedRelation {
  implicit val extractedRelationFormat = Json.format[ExtractedRelation]
}

