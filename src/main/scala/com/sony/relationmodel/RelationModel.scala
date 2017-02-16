package com.sony.relationmodel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import se.lth.cs.docforia.Document

/**
  * Created by erik on 2017-02-13.
  */
object RelationModel {

  def train(relation: Dataset[Relation], docs: RDD[Document]): RelationModel = {
    new RelationModel()
  }

}

class RelationModel {

}
