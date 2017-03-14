package com.sony.prometheus.evaluation

import org.apache.spark.SparkContext
import com.sony.prometheus.pipeline._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.annotaters.VildeAnnotater
import se.lth.cs.docforia.Document

class EvaluationData(path: String)(implicit sc: SparkContext) extends Data {
  override def getData(): String = {
    if (exists(path))
      path
    else
      throw new Exception("Evaluation Data missing")
  }
}

case class Judgment(
  judgment: String,
  rater: String)

case class Evidence(
  snippet: String,
  url: String)

case class EvaluationDataPoint(
  wd_sub: String,
  sub: String,
  judgments: Seq[Judgment],
  pred: String,
  evidences: Seq[Evidence],
  wd_obj: String,
  obj: String,
  wd_pred: String)

object EvaluationDataReader {

  /** Returns an RDD of [[EvaluationDataPoint]] read from path
    *
    */
  def load(path: String)(implicit sqlContext: SQLContext): RDD[EvaluationDataPoint]  = {
    import sqlContext.implicits._
    sqlContext.read.json(path).as[EvaluationDataPoint].rdd
  }

  def getAnnotatedDocs(path: String)(implicit sqlContext: SQLContext): RDD[Document] = {
    load(path).flatMap(p => {
      p.evidences.map(_.snippet).map(s => {
        VildeAnnotater.annotatedDocument(s, conf = "herd")
      })
    })
  }
}


