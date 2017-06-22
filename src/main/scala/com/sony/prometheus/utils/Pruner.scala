package com.sony.prometheus.utils

import com.sony.prometheus.stages.FeatureExtractor.log
import com.sony.prometheus.stages._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object Pruner {

  private var broadCastedPruner: Broadcast[Pruner]  = null

  def apply(relationConfig: String)(implicit sqlContext:SQLContext): Broadcast[Pruner] = {

    if(broadCastedPruner == null) {
      val types = RelationConfigReader.load(relationConfig)
        .filter(_.types.length == 2)
        .map(r => r.classIdx.toLong -> (r.types(0).toLowerCase, r.types(1).toLowerCase))
        .toMap
      broadCastedPruner = sqlContext.sparkContext.broadcast(new Pruner(types))
    }

    broadCastedPruner
  }

}

class Pruner(types: Map[Long, (String, String)]) extends Serializable{

  def pruneTrainingData(data: RDD[TrainingDataPoint])
                       (implicit sqlContext: SQLContext): RDD[TrainingDataPoint] = {

    log.info(s"Training Data Pruner - Initial size: ${data.count}")

    /* Filter out points without any dependency paths */
    var prunedData = data.filter(d => {
      d.dependencyPath.nonEmpty
    })
    log.info(s"Training Data Pruner - Empty dependency paths: ${prunedData.count}")

    prunedData = prunedData.filter(d => {
      /* Filter points without correct entity types. */
      if (d.relationId == FeatureExtractor.NEGATIVE_CLASS_NAME) {
        true
      } else {
        isValidTypes(d.ent1Type, d.ent2Type, d.relationClass, d.ent1IsSubject)
      }
    })
    log.info(s"Training Data Pruner - Correct NE types: ${prunedData.count}")

    prunedData
  }
  /**
    * Performs a sanity check to prune away bad predictions
    */
  def prunePrediction(prediction: Prediction, point: TestDataPoint)(implicit sqlContext: SQLContext): Boolean = {
    isValidTypes(point.ent1Type, point.ent2Type, prediction.clsIdx, point.ent1IsSubject)
  }

  def isValidTypes(ent1Type: String, ent2Type: String, classID: Long, ent1IsSubject: Boolean)
                  (implicit sqlContext: SQLContext): Boolean = {
    types.get(classID).forall(t => {
      val e1 = if(ent1IsSubject) ent1Type.toLowerCase else ent2Type.toLowerCase
      val e2 = if(ent1IsSubject) ent2Type.toLowerCase else ent1Type.toLowerCase
      val expected1 = if(t._1 != "*") t._1 else e1
      val expected2 = if(t._2 != "*") t._2 else e2
      e1 == expected1 && e2 == expected2
    })
  }

}
