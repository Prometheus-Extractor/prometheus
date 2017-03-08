package com.sony.prometheus.annotaters

import scalaj.http._
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.memstore.MemoryDocumentIO

/** Provides a way to annotate strings into docforia Documents
 */
object VildeAnnotater extends Annotater {
  override def annotatedDocument(input: String, lang: String = "sv", conf: String = "default"): Document = {
    val vildeURL = s"http://vilde.cs.lth.se:9000/$lang/$conf/api/json"
    val response: HttpResponse[String] = Http(vildeURL)
      .postData(input)
      .header("content-type", "application/json")
      .asString
    val docJson = response.body
    MemoryDocumentIO.getInstance().fromJson(docJson)
  }
}

