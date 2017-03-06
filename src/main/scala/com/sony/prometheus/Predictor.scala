package com.sony.prometheus

import com.sony.prometheus.pipeline.{Data, Task}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Sentence

import scala.collection.JavaConverters._

/**
  * Created by erik on 2017-02-28.
  */
object Predictor {

  def apply(modelStage: Data, featureTransformer: Data)
           (implicit sqlContext: SQLContext): Predictor = {

    val model = RelationModel.load(modelStage.getData(), sqlContext.sparkContext)
    val transformer = FeatureTransformer.load(featureTransformer.getData())

    new Predictor(model, transformer)
  }

}

class Predictor(model: RelationModel, transformer: FeatureTransformer) extends Serializable{

  def extractRelations(docs: RDD[Document])(implicit sqlContext: SQLContext) = {

    docs.flatMap(doc => {
      val sentences = doc.nodes(classOf[Sentence])
        .asScala
        .toSeq
        .map(s => doc.subDocument(s.getStart, s.getEnd))

      val points: Seq[TestDataPoint] = FeatureExtractor.testData(transformer, sentences)
      val classes = points.map(p => RelationModel.oneHotEncode(p.features, transformer.vocabSize())).map(model.predict)

      classes.zip(points).map{
        case (result: Double, point: TestDataPoint) =>
          ExtractedRelation(point.qidSource, result, point.qidDest, point.sentence.text(), doc.id(), -1.0)
      }
    })
  }

}

case class ExtractedRelation(subject: String, predictedPredicate: Double, obj: String, sentence: String, source: String, probability: Double)
