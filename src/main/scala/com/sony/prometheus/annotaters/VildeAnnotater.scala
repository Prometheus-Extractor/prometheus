package com.sony.prometheus.annotaters

import com.sony.prometheus.Prometheus
import com.sony.prometheus.stages.Coref

import scalaj.http.{Http, HttpResponse}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text._
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.QueryCollectors

import scala.collection.JavaConverters._


/** Provides a way to annotate strings into Docforia Documents
  */
object VildeAnnotater extends Annotater {
  override def annotate(input: String, lang: String, conf: String): Either[String, Document] = {
    val CONNECTION_TIMEOUT = 5000
    val READ_TIMEOUT = 60000
    val vildeURL = s"http://vilde.cs.lth.se:9000/$lang/$conf/api/json"
    try {
      val response: HttpResponse[String] = Http(vildeURL)
        .timeout(connTimeoutMs = CONNECTION_TIMEOUT, readTimeoutMs = READ_TIMEOUT)
        .postData(input)
        .header("content-type", "application/json; charset=UTF-8")
        .asString

      val docJson = response.body
      var doc: Document = MemoryDocumentIO.getInstance().fromJson(docJson)
      if (Prometheus.conf.corefs())
        doc = Coref.propagateCorefs(doc)
      
      Right(doc)
    } catch {
      case e: java.net.SocketTimeoutException => Left(e.getMessage)
    }
  }

}
