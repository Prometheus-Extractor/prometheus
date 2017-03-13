package com.sony.prometheus.evaluation

import org.apache.spark.SparkContext
import com.sony.prometheus.pipeline._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class EvaluationData(path: String)(implicit sc: SparkContext) extends Data {
  override def getData(): String = {
    if (exists(path))
      path
    else
      throw new Exception("Evaluation Data missing")
  }
}
// example evaluationDataPoint json object
// {"wd_sub": "Q3388789", "sub": "/m/026_tl9", "judgments": [{"judgment": "yes", "rater": "11595942516201422884"}, {"judgment": "yes", "rater": "16169597761094238409"}, {"judgment": "yes", "rater": "1014448455121957356"}, {"judgment": "yes", "rater": "16651790297630307764"}, {"judgment": "yes", "rater": "1855142007844680025"}], "pred": "/people/person/place_of_birth", "evidences": [{"url": "http://en.wikipedia.org/wiki/Morris_S._Miller", "snippet": "Morris Smith Miller (July 31, 1779 -- November 16, 1824) was a United States Representative from New York. Born in New York City, he graduated from Union College in Schenectady in 1798. He studied law and was admitted to the bar. Miller served as private secretary to Governor Jay, and subsequently, in 1806, commenced the practice of his profession in Utica. He was president of the village of Utica in 1808 and judge of the court of common pleas of Oneida County from 1810 until his death."}], "wd_obj": "Q60", "obj": "/m/02_286", "wd_pred": "P19"}

case class EvaluationDataPoint(
  wd_sub: String,
  sub: String,
  judgements: Seq[Tuple2[String,String]], // this is probably wrong
  pred: String,
  evidences: Seq[Tuple2[String, String]], // this is also probably wrong
  wd_obj: String,
  obj: String,
  wd_pred: String)

object EvaluationDataReader {

  /** Returns a Seq of [[EvaluationDataPoint]], an empty Seq if json deserializing
   *  fails
   */
  def load(path: String)(implicit sqlContext: SQLContext): RDD[EvaluationDataPoint]  = {
    import sqlContext.implicits._
    sqlContext.read.json(path).as[EvaluationDataPoint].rdd
  }
}


