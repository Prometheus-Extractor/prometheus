package com.sony.prometheus.interfaces

import org.http4s._
import org.http4s.dsl._
import org.http4s.server.{Server}
import scalaz.concurrent.{Task => HttpTask}
import org.http4s.headers.{`Content-Type`, `Content-Length`}
import org.http4s.MediaType._
import com.sony.prometheus.annotaters._
import org.apache.spark.{SparkContext}
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.Predictor

object REST {
  def api(task: Server, predictor: Predictor)(implicit sc: SparkContext, sqlContext: SQLContext) = HttpService {
    case req @ POST -> Root / "extract" =>
      val is = scalaz.stream.io.toInputStream(req.body)
      val input = scala.io.Source.fromInputStream(is).getLines().mkString("\n")
      val doc = VildeAnnotater.annotatedDocument(input)
      predictor.extractRelations(sc.parallelize(List(doc)))
      println(doc)
      Ok(req.body).putHeaders(`Content-Type`(`text/plain`))
    case GET -> Root / "hello" / name =>
      Ok(s"Hello REST $name")
    case GET -> Root / "shutdown" =>
      // Does not work unfortunately
      HttpTask({
        Thread.sleep(2000)
        task.shutdownNow()
      }).runAsync(_)
      Ok("Goodbye, cruel world")
  }
}
