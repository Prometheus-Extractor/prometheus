package com.sony.prometheus.evaluation

import com.sony.prometheus.utils.Utils.pathExists
import org.apache.spark.SparkContext
import com.sony.prometheus.stages._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.annotators.VildeAnnotater
import org.apache.log4j.LogManager
import play.api.libs.json.Json
import se.lth.cs.docforia.Document

class ModelEvaluationData(path: String)(implicit sc: SparkContext) extends Data {
  override def getData(): String = {
    if (pathExists(path)) {
      path
    } else {
      throw new Exception("Evaluation Data missing")
    }
  }
}

case class Judgment(
  judgment: String,
  rater: String)

object Judgment {
  implicit val format = Json.format[Judgment]
}

case class Evidence(
  snippet: String,
  url: String)

object Evidence {
  implicit val format = Json.format[Evidence]
}


/** Data structure for both true and false examples of extracted relations and
  * their sources in the form of text snippets
  */
case class ModelEvaluationDataPoint(
  wd_sub: String,
  sub: String,
  judgments: Seq[Judgment],
  pred: String,
  evidences: Seq[Evidence],
  wd_obj: String,
  obj: String,
  wd_pred: String) {
  def positive(): Boolean = {judgments.count(_.judgment == "yes") > judgments.length / 2.0}
}

object ModelEvaluationDataPoint {
  implicit val format = Json.format[ModelEvaluationDataPoint]
}

object ModelEvaluationDataReader {
  val log = LogManager.getLogger(ModelEvaluationDataReader.getClass)

  /** Returns an RDD of [[ModelEvaluationDataPoint]] read from path
    *
    */
  def load(path: String)(implicit sqlContext: SQLContext): RDD[ModelEvaluationDataPoint]  = {
    import sqlContext.implicits._
    sqlContext.read.json(path).as[ModelEvaluationDataPoint].rdd
  }

  /** Retuns and RDD of HERD-annotated Document:s, read from file containing
    * [[ModelEvaluationDataPoint]]:s
    *
    * @param path       the path to the file to read
    * @return          RDD of HERD-annotated Document:s; one Document per
    * evidence snippet per EvaluationDataPoint in the file
   */
  def getAnnotatedDocs(path: String)(implicit sqlContext: SQLContext): RDD[Document] = {
    load(path).flatMap(p => {
      p.evidences.map(_.snippet).flatMap(s => {
        VildeAnnotater.annotate(s, conf = "herd") match {
          case Right(doc) => Seq(doc)
          case Left(msg) => {
            log.error(msg)
            Seq()
          }
        }
      })
    })
  }
}


