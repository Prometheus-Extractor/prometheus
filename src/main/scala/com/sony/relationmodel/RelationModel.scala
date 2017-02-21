package com.sony.relationmodel

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import se.lth.cs.docforia.Document

class RelationModelStage(path: String, featureExtractor: Data)
                        (implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(force: Boolean = false): String = {
    if (!exists(path) || force) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val data:DataFrame = FeatureExtractor.load(featureExtractor.getData())



  }
}

object RelationModel {



}

class RelationModel(relation: String) {

}
