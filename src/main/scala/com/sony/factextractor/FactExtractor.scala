import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.memstore.MemoryDocumentIO

object FactExtractor{

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    val conf = new SparkConf().setAppName("Fact Extractor")
    conf.setIfMissing("master", "local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df:DataFrame = sqlContext.read.parquet(args(0))
    log.debug(df.schema.mkString)

    val ioErrors:Accumulator[Int] = sc.accumulator(0, "IO_ERRORS")

    // we might need to filter for only articles here but that wouldn't be a generelized solution.

    df.flatMap(row => {
      val doc: Document = MemoryDocumentIO.getInstance().fromBytes(row.getAs(5):Array[Byte])
      return doc
    })


    // code to do stuff
    sc.stop()
  }

}
