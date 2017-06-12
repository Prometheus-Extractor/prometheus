package com.sony.prometheus.interfaces

import org.http4s._
import org.http4s.dsl._
import org.http4s.headers.`Content-Type`
import org.http4s.MediaType._
import com.sony.prometheus.annotaters._
import com.sony.prometheus.stages.Predictor
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import play.api.libs.json._
import java.nio.charset.{Charset, CodingErrorAction}

object REST {
  def api(predictor: Predictor)
         (implicit sc: SparkContext, sqlContext: SQLContext): HttpService = HttpService {
    case req @ POST -> Root / "api" / lang / "extract" =>
      val is = scalaz.stream.io.toInputStream(req.body)
      val decoder = Charset.forName("UTF-8").newDecoder()
      decoder.onMalformedInput(CodingErrorAction.IGNORE)
      val input = scala.io.Source.fromInputStream(is)(decoder).getLines().mkString("\n")
      VildeAnnotater.annotate(input, lang = lang, conf = "herd") match {
        case Right(doc) => {
          val results = predictor
            .extractRelationsLocally(Seq(doc))
            .flatMap(rels => rels.filter(!_.predictedPredicate.contains(predictor.UNKNOWN_CLASS)))
          val res = Json.toJson(results).toString
          Ok(res).putHeaders(`Content-Type`(`application/json`))
        }
        case Left(msg) => {
          LogManager.getLogger("REST Api").error(s"Error while handling REST api request: $msg")
          InternalServerError(msg)
        }
      }

    case GET -> Root =>
      Ok("POST text to /api/<lang>/extract to extract relations.")
  }
}
