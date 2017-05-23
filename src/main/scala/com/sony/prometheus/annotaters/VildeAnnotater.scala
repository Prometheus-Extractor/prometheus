package com.sony.prometheus.annotaters

import scalaj.http.{Http, HttpResponse}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.{CoreferenceChain, CoreferenceChainEdge, CoreferenceMention, Token}
import se.lth.cs.docforia.memstore.MemoryDocumentIO
import se.lth.cs.docforia.query.QueryCollectors

import scala.collection.JavaConverters._


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

  /** Resolve any coreference chains in doc by copying over the named entity to the mentions
    */
  private def resolveCorefs(doc: Document): Document = {
    val T = Token.`var`()
    val M = CoreferenceMention.`var`()
    val NED = NamedEntityDisambiguation.`var`()

    doc.select(T, M, NED).where(T).coveredBy(M).where(NED).coveredBy(M)
      .stream()
      .collect(QueryCollectors.groupBy(doc, M).values(T).collector())
      .asScala
      .foreach(pg => {
        val mention = pg.key(M)
        val corefs = mention
          .connectedEdges(classOf[CoreferenceChainEdge]).asScala
          .flatMap(edge => edge.getHead[CoreferenceChain].connectedNodes(classOf[CoreferenceMention]).asScala)

        val ned = pg.key(NED)
        corefs.filter(m => m.getProperty("mention-type") != "PROPER").foreach(m => {
          // TODO: add label?
          new NamedEntityDisambiguation(doc)
            .setRange(m.getStart, m.getEnd)
            .setIdentifier(ned.getIdentifier)
            .setScore(ned.getScore)
          m
        })
      })
    doc
  }
}
