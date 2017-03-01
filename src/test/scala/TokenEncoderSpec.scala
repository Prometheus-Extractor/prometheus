import org.scalatest.{FlatSpec, BeforeAndAfter, Matchers}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.memstore.MemoryDocument
import se.lth.cs.docforia.graph.text.Token
import scala.collection.JavaConverters._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import com.sony.prometheus.TokenEncoder

class TokenEncoderSpec extends FlatSpec with BeforeAndAfter with Matchers {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val conf = new SparkConf().setAppName("TEST")
  conf.setMaster("local[*]")
  val sc = new SparkContext(conf)

  after {
    sc.stop()
  }

  trait TestDocument {
    val stringDoc = "Apache Spark's StringIndexer is inferior to TokenEncoder!"
    val mDoc: Document = new MemoryDocument(stringDoc)
    // build Token:s
    val words = stringDoc.split(" ")
    val tokens = words.map(w => (stringDoc.indexOfSlice(w), w.length)).map(idxPair => {
      new Token(mDoc).setRange(idxPair._1, idxPair._1 + idxPair._2)
    })
    val docs = sc.parallelize(Seq(mDoc))

  }

  "A TokenEncoder" should "uniquely encode strings" in new TestDocument {
    val te = TokenEncoder(docs)
    words.map(token => {
      println(s"$token has index: ${te.index(token)}")
      te.index(token)
    }) should equal (words.zipWithIndex.map(_._2))
  }
}

