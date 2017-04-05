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

class PredictorStage(
  path: String,
  modelStage: Data,
  posEncoder: Data,
  word2VecData: Word2VecData,
  relationsData: Data,
  docs: RDD[Document])
  (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path))
      run()
    path
  }

  override def run(): Unit = {

    val predictor = Predictor(modelStage, posEncoder: Data, word2VecData: Word2VecData, relationsData)
    val data = predictor.extractRelations(docs)
    Predictor.save(data, path)
  }
}

/**
  * Created by erik on 2017-02-28.
  */
object Predictor {

  def apply(modelStage: Data, posEncoder: Data, word2VecData: Word2VecData, relationData: Data)
           (implicit sqlContext: SQLContext): Predictor = {
    val featureTransformer = FeatureTransformer(word2VecData.getData(), posEncoder.getData())
    val model = RelationModel.load(modelStage.getData(), sqlContext.sparkContext)
    val relations = RelationsReader.readRelations(relationData.getData())
    new Predictor(model, featureTransformer, relations)
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

class Predictor(model: RelationModel, transformer: FeatureTransformer, relations: RDD[Relation]) extends Serializable {

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
        .map(p => transformer.toFeatureVector(p.wordFeatures, p.posFeatures))
        .map(model.predict)

      classes.zip(points).map{
        case (result: Double, point: TestDataPoint) =>
          val predicate = classIdxToId.getOrElse(result.round.toInt, s"$UNKNOWN_CLASS: $result>")
          ExtractedRelation(point.qidSource, predicate, point.qidDest, point.sentence.text(), doc.uri(), -1.0)
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

