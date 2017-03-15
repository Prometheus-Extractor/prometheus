import org.scalatest.{FlatSpec, Matchers}
import se.lth.cs.docforia.memstore.MemoryDocument
import se.lth.cs.docforia.Document
import com.sony.prometheus.annotaters._
import scala.collection.JavaConverters._
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.graph.text.Token
import se.lth.cs.docforia.query.QueryCollectors

class VildeAnnotaterSpec extends FlatSpec with Matchers {

  "The VildAnnotater" should "produce a herd-annoted docforia Document" in {
    val str = "Barack Obama gifte sig med Michelle Obama."
    val doc = VildeAnnotater.annotate(str, conf = "herd")

    val NED = NamedEntityDisambiguation.`var`()
    val T = Token.`var`()
    doc.select(NED, T)
      .where(T)
      .coveredBy(NED)
      .stream()
      .collect(QueryCollectors.groupBy(doc, NED).values(T).collector())
      .asScala
      .toList
      .length should be (2)
  }
}

