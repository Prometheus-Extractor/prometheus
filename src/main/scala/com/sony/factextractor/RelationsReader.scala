package com.sony.factextractor

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.rdd.RDD

object RelationsReader {
  def readRelations(sqlContext: SQLContext, file: String): DataFrame = {
    sqlContext.read.json(file)
  }
}


