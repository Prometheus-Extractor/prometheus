package com.sony.prometheus.stages

import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text._
import se.lth.cs.docforia.query.QueryCollectors

import scala.collection.JavaConverters._

/**
  * Created by erik on 2017-06-13.
  */
object Coref {

  /** Resolve any coreference chains in doc by copying over the named entity to the mentions
    */
  def propagateCorefs(doc: Document): Document = {
    val T = Token.`var`()
    val M = CoreferenceMention.`var`()
    val NED = NamedEntityDisambiguation.`var`()
    val NE = NamedEntity.`var`()

    doc.select(T, M, NED, NE).where(T).coveredBy(M).where(NED, NE).coveredBy(M)
      .stream()
      .collect(QueryCollectors.groupBy(doc, M, NED, NE).values(T).collector())
      .asScala
      .foreach(pg => {
        val mention = pg.key(M)
        val ne = pg.key(NE)
        val corefs = mention
          .connectedEdges(classOf[CoreferenceChainEdge]).asScala
          .flatMap(edge => edge.getHead[CoreferenceChain].connectedNodes(classOf[CoreferenceMention]).asScala)

        val ned = pg.key(NED)
        corefs.filter(m => m.getProperty("mention-type") != "PROPER").foreach(m => {
          val newNe = new NamedEntity(doc)
            .setRange(m.getStart, m.getEnd)
            .setLabel(ne.getLabel)
          val newNed = new NamedEntityDisambiguation(doc)
            .setRange(m.getStart, m.getEnd)
            .setIdentifier(ned.getIdentifier)
            .setScore(ned.getScore)
          if (ned.hasProperty("LABEL"))
            newNed.putProperty("LABEL", ned.getProperty("LABEL"))
          m
        })
      })
    doc
  }

}
