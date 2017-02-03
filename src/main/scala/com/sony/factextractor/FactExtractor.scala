import org.apache.spark.{SparkConf, SparkContext}

object FactExtractor{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Fact Extractor")
    conf.setIfMissing("master", "local[*]")
    val sc = new SparkContext(conf)

    // code to do stuff

    sc.stop()
  }

}
