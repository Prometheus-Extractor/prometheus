import org.scalatest.{FlatSpec, BeforeAndAfter, Matchers}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.memstore.MemoryDocument
import se.lth.cs.docforia.graph.text.{Sentence, Token}
import scala.collection.JavaConverters._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import com.sony.prometheus.evaluation._
import com.holdenkarau.spark.testing.SharedSparkContext

class EvaluationSpec extends FlatSpec with BeforeAndAfter with Matchers with SharedSparkContext {
  "EvaluationDataReader" should "read json file properly" in {
    implicit val sqlContext = new SQLContext(sc)
    val edPointsRDD: RDD[EvaluationDataPoint] = EvaluationDataReader.load("./src/test/data/evaluationTest.txt")
    val edPoints = edPointsRDD.collect()
    edPoints(0).wd_sub should equal ("Q3388789")
    edPoints(0).wd_obj should equal ("Q60")
    edPoints(0).wd_pred should equal ("P19")
    edPoints(0).obj should equal ("/m/02_286")
    edPoints(0).sub should equal ("/m/026_tl9")
    val j0 = edPoints(0).judgments(0)
    j0.judgment should equal ("yes")
    j0.rater should equal ("11595942516201422884")
    val j1 = edPoints(0).judgments(1)
    j1.judgment should equal ("yes")
    j1.rater should equal ("16169597761094238409")
    val e0 = edPoints(0).evidences(0)
    e0.url should equal ("http://en.wikipedia.org/wiki/Morris_S._Miller")
  }

  "EvaluationDataReader" should "extract snippets into annotated docs" in {
    implicit val sqlContext = new SQLContext(sc)
    val docs: RDD[Document] = EvaluationDataReader.getAnnotatedDocs("./src/test/data/evaluationTest.txt")
    println(docs.collect().mkString("\n"))
  }
}

