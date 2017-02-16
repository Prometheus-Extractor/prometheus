package com.sony.relationmodel

import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import org.apache.spark.rdd.RDD

case class Relation(name: String, id: String, entities: Seq[EntityPair] = List())
case class EntityPair(source: String, dest: String)

object RelationsReader {
  def readRelations(file: String)(implicit sqlContext: SQLContext): RDD[Relation] = {
    import sqlContext.implicits._
    sqlContext.read.parquet(file).as[Relation].rdd
  }
}


