package com.sony.prometheus

import com.sony.prometheus.pipeline.{Data, Task}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Sentence
import play.api.libs.json._
import scala.collection.JavaConverters._

class PredictorStage(
  path: String,
  modelStage: Data,
  featureTransformer: Data,
  relationsData: Data,
  docs: RDD[Document])
  (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!exists(path))
      run()
    path
  }

  override def run(): Unit = {
    val predictor = Predictor(modelStage, featureTransformer, relationsData)
    val data = predictor.extractRelations(docs)
    Predictor.save(data, path)
  }
}

/**
  * Created by erik on 2017-02-28.
  */
object Predictor {

  def apply(modelStage: Data, featureTransformer: Data, relationData: Data)
           (implicit sqlContext: SQLContext): Predictor = {

    val model = RelationModel.load(modelStage.getData(), sqlContext.sparkContext)
    val transformer = FeatureTransformer.load(featureTransformer.getData())

    val relations = RelationsReader.readRelations(relationData.getData())
    new Predictor(model, transformer, relations)
  }

  def load(path: String)(implicit sqlContext: SQLContext): RDD[ExtractedRelation] = {
    import sqlContext.implicits._
    sqlContext.read.json(path).as[ExtractedRelation].rdd
  }

  def save(data: RDD[ExtractedRelation], path: String)(implicit sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._
    data.toDF().write.json(path)
  }

}

class Predictor(model: RelationModel, transformer: FeatureTransformer, relations: RDD[Relation]) extends Serializable {

  def extractRelations(docs: RDD[Document])(implicit sqlContext: SQLContext): RDD[ExtractedRelation] = {

    val broadcastedFT = relations.sparkContext.broadcast(transformer)

    val classIdxToId: Map[Int, String] = relations.map(r => (r.classIdx, r.id)).collect().toList.toMap

    docs.flatMap(doc => {
      val sentences = doc.nodes(classOf[Sentence])
        .asScala
        .toSeq
        .map(s => {
          if (doc.id() == null) {
            // This is a work around for a bug in Docforia.
            doc.setId("<null_id>")
          }
        doc.subDocument(s.getStart, s.getEnd)})

      val points: Seq[TestDataPoint] = FeatureExtractor.testData(transformer, sentences)
      val classes = points.map(p => broadcastedFT.value.toFeatureVector(p.wordFeatures, p.posFeatures)).map(model.predict)

      classes.zip(points).map{
        case (result: Double, point: TestDataPoint) =>
          val predicate = classIdxToId.getOrElse(result.round.toInt, s"<unknown_class: $result>")
          ExtractedRelation(point.qidSource, predicate, point.qidDest, point.sentence.text(), doc.uri(), -1.0)
      }
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

