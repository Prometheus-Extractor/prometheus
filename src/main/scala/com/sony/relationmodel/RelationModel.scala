package com.sony.relationmodel

import org.apache.spark.rdd.RDD

/**
  * Created by erik on 2017-02-13.
  */
object RelationModel {

  def train(relation: RDD[RelationRow], docs: RDD[RelationRow]): RelationModel = {
    return new RelationModel()
  }

}

class RelationModel {

}
