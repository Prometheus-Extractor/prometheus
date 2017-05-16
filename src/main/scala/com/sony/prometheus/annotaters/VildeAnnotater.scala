package com.sony.prometheus.annotaters

import scalaj.http._
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.memstore.MemoryDocumentIO

/** Provides a way to annotate strings into Docforia Documents
  */
object VildeAnnotater extends Annotater {
  override def annotate(input: String, lang: String, conf: String): Either[String, Document] = {
    val CONNECTION_TIMEOUT = 2000
    val READ_TIMEOUT = 10000
    val vildeURL = s"http://vilde.cs.lth.se:9000/$lang/$conf/api/json"
    try {
      val response: HttpResponse[String] = Http(vildeURL)
        .timeout(connTimeoutMs = CONNECTION_TIMEOUT, readTimeoutMs = READ_TIMEOUT)
        .postData(input)
        .header("content-type", "application/json; charset=UTF-8")
        .asString

      val docJson = response.body
      Right(MemoryDocumentIO.getInstance().fromJson(docJson))
    } catch {
      case e: java.net.SocketTimeoutException => Left(e.getMessage)
    }
  }
}
