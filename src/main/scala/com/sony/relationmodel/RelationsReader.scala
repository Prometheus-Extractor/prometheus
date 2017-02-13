package com.sony.relationmodel

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.rdd.RDD

case class RelationRow(relationName: String, relationId: String, entity1: String, entity2: String)

object RelationsReader {
  def readRelations(sqlContext: SQLContext, file: String):RDD[RelationRow] = {
    sqlContext.read.parquet(file).map(row => {
      RelationRow(row.getString(0), row.getString(1), row.getString(2), row.getString(3))
    })
  }
}


