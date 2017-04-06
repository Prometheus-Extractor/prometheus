package com.sony.prometheus.stages

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.utils.Utils.pathExists

case class Relation(name: String, id: String, classIdx: Int, entities: Seq[EntityPair] = Seq())
case class EntityPair(source: String, dest: String)

/** Represents the relations data
 */
class RelationsData(path: String)(implicit sc: SparkContext) extends Data {
  override def getData(): String = {
    if (pathExists(path)) {
      path
    } else {
      throw new Exception("Relations data missing")
    }
  }
}

/** Reads relations data file into an RDD of [[Relation]]
 */
object RelationsReader {
  def readRelations(file: String)(implicit sqlContext: SQLContext): RDD[Relation] = {
    import sqlContext.implicits._
    sqlContext.read.parquet(file).as[Relation].rdd
  }
}

