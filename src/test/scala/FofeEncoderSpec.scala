import com.sony.prometheus.utils.FofeEncoder
import org.apache.spark.mllib.linalg.Vectors
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by erik on 2017-08-10.
  */

class FofeEncoderSpec extends FlatSpec with Matchers {

  trait TestVectors {
    val v1 = Vectors.dense(1.0, 0.0, 0.0)
    val v2 = Vectors.dense(0.0, 1.0, 0.0)
    val v3 = Vectors.dense(0.0, 0.0, 1.0)
  }

  "A FofeEncoder" should "should encode v1 as expected" in new TestVectors {

    val alpha = 0.5
    val fofeEncoder = new FofeEncoder(alpha)
    val res = fofeEncoder.encodeRight(Seq(v1, v2, v3))
    val expected = Vectors.dense(1, alpha, alpha * alpha)

    res.toArray should equal (expected.toArray)
  }
}
