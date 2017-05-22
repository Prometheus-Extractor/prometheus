package com.sony.prometheus.annotaters

import java.util.stream.Collectors

import scalaj.http.{Http, HttpResponse}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.{CoreferenceChain, CoreferenceChainEdge, CoreferenceMention, Token}
import se.lth.cs.docforia.memstore.{MemoryDocument, MemoryDocumentIO}
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
  private def resolveCoref(doc: Document): Document = {
    val M = CoreferenceMention.`var`()
    val EDGE = CoreferenceChainEdge.`var`()
    val NED = NamedEntityDisambiguation.`var`()
    val T = Token.`var`()



    val nedGroups = doc.select(T, M, NED).where(T).coveredBy(M)
      .stream()
      .collect(QueryCollectors.groupBy(doc, M).values(T).collector())
      .asScala
      .map(pg => {
        val what = pg.key().get(M).connectedEdges(classOf[CoreferenceChainEdge]).asScala.toList
        val corefs = what.flatMap(edge => edge.getHead[CoreferenceChain].connectedNodes(classOf[CoreferenceMention]).asScala.toList)
        val thisCoref = pg.key(M)
        val ned = corefs.filter(coref => coref.getProperty("mention-type") == "PROPER").headOption.get.getL
        //val newNed = new NamedEntityDisambiguation(doc).setRange(thisCoref.getStart, thisCoref.getEnd).setLabel(ned.get.)

        (thisCoref, corefs, ned)
      })
      .toList


    ???
  }
}
