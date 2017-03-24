package com.sony.prometheus.interfaces

import org.http4s._
import org.http4s.dsl._
import org.http4s.headers.{ `Content-Type`}
import org.http4s.MediaType._
import com.sony.prometheus.annotaters._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.Predictor
import play.api.libs.json._

object REST {
  def api(predictor: Predictor)
         (implicit sc: SparkContext, sqlContext: SQLContext): HttpService = HttpService {
    case req @ POST -> Root / "api" / "extract" =>
      val is = scalaz.stream.io.toInputStream(req.body)
      val input = scala.io.Source.fromInputStream(is).getLines().mkString("\n")
      val doc = VildeAnnotater.annotate(input, lang = "sv", conf = "herd")
      val results = predictor
        .extractRelations(sc.parallelize(List(doc)))
        .map(rels => rels.filter(_.predictedPredicate != "<unknown_class: 0.0>"))
      val res = Json.toJson(results.collect()).toString
      Ok(res).putHeaders(`Content-Type`(`application/json`))
    case GET -> Root =>
      Ok("POST text to /api/extract to extract relations.")
  }
}
