package com.sony.prometheus.interfaces

import org.http4s._
import org.http4s.dsl._
import org.http4s.server.Server

import scalaz.concurrent.{Task => HttpTask}
import org.http4s.headers.{ `Content-Type`}
import org.http4s.MediaType._
import com.sony.prometheus.annotaters._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.Predictor
import play.api.libs.json._

object REST {
  def api(task: Server, predictor: Predictor)(implicit sc: SparkContext, sqlContext: SQLContext) = HttpService {
    case req @ POST -> Root / "extract" =>
      val is = scalaz.stream.io.toInputStream(req.body)
      val input = scala.io.Source.fromInputStream(is).getLines().mkString("\n")
      val doc = VildeAnnotater.annotatedDocument(input, conf = "herd")
      val results = predictor.extractRelations(sc.parallelize(List(doc)))
      val res = Json.toJson(results.collect()).toString
      Ok(res).putHeaders(`Content-Type`(`application/json`))

    case GET -> Root / "shutdown" =>
      // Does not work unfortunately
      HttpTask({
        Thread.sleep(2000)
        task.shutdownNow()
      }).runAsync(_)
      Ok("Goodbye, cruel world")

  }
}
