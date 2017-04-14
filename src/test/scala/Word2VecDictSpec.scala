import java.io.{File, FileInputStream}
import java.nio.{ByteBuffer, ByteOrder}

import com.holdenkarau.spark.testing.SharedSparkContext
import com.sony.prometheus.stages.Word2VecDict
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by erik on 2017-04-14.
  */
class Word2VecDictSpec extends FlatSpec with BeforeAndAfter with Matchers with SharedSparkContext {

  trait testFiles {
    val vocabFile = new File("../../data/word2vec/en/model.opt.vocab")
    val vecsFile = new File("../../data/word2vec/en/model.opt.vecs")
  }

  "Standard Load Vectors" should "be same as fast load" in new testFiles {
    implicit val sqlContext = new SQLContext(sc)
    implicit val context = sc
    val my = TestLoader.directLoadVectors(vecsFile.getAbsolutePath(), 100)
    val dict = Word2VecDict(vocabFile, vecsFile)

    my(0) should equal (dict.vector(0))
    my(1) should equal (dict.vector(1))
    my(3) should equal (dict.vector(3))

  }

}


object TestLoader {
  def directLoadVectors(path: String, vocabSize: Int)(implicit context: SparkContext): Array[Array[Float]] = {
    val fs = FileSystem.get(context.hadoopConfiguration)
    val input = fs.open(new Path("file://" + path))

    var buff = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    var bytes = new Array[Byte](4)

    input.read(bytes)
    buff.put(bytes)
    buff.rewind()

    val dimensions = buff.asIntBuffer().get()
    val matrix : Array[Array[Float]] = Array.ofDim[Float](vocabSize, dimensions)

    System.out.println("Start reading...")
    for(i <- (0 until vocabSize)) {
      buff = ByteBuffer.allocate(dimensions * 4).order(ByteOrder.LITTLE_ENDIAN)
      bytes = new Array[Byte](dimensions * 4)
      input.read(bytes)
      buff.put(bytes)
      buff.rewind()
      buff.asFloatBuffer().get(matrix(i))
    }
    input.close()
    matrix
  }
}
