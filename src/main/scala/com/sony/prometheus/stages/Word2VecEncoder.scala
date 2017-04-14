package com.sony.prometheus.stages

import java.io.{Externalizable, File, ObjectInput, ObjectOutput}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.nio.file.{Paths, StandardOpenOption}

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.log4j.LogManager
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.sony.prometheus.utils.Utils.pathExists
import org.apache.hadoop.fs.{FileSystem, Path}

class Word2VecData(path: String)(implicit sc: SparkContext) extends Data {

  override def getData(): String = {
    if(pathExists(path)) {
      path
    } else {
      throw new Exception(s"Missing Word2Vec model $path")
    }
  }

}

/**
  * Created by erik on 2017-03-22.
  */
object Word2VecEncoder {

  def apply(modelPath: String): Word2VecEncoder = {
    val models = Word2VecDict(modelPath + "/model.opt.vocab", modelPath + "/model.opt.vecs")
    val word2vec = new Word2VecEncoder(models)
  }
}

/**
  * This class wraps the specific Word2Vec implementation. We've tried Spark's, DL4J's and now Marcus'.
  * @param model
  */
class Word2VecEncoder(model: Word2VecDict) {

  def index(token: String): Vector = {
    val idx = model.vectorIdx(token)
    if(idx == -1){
      Vectors.zeros(model.dim)
    }else{
      Vectors.dense(model.vector(idx).map(_.toDouble))
    }
  }

  def vectorSize(): Int = {model.dim}

}


object Word2VecDict {

  def apply(vocabFile : String, vecsFile: String, unknownWord : String="__UNKNOWN__")
           (implicit context: SparkContext): Word2VecDict  = {

    val vocab = loadVocab(vocabFile, unknownWord)
    val vecs = directLoadVectors(vecsFile, vocab.size)

    new Word2VecDict(vocab, vecs)
  }

  private def directLoadVectors(path: String, vocabSize: Int)(implicit context: SparkContext): Array[Array[Float]] = {
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

  /**
    * Load the vocabulary
    * @param path the path to the vocabulary
    * @return initilized vocabulary index
    */
  private def loadVocab(path : String, unknownWord: String)
                       (implicit context: SparkContext) : Object2IntOpenHashMap[String] = {
    //var lines = Files.lines(Paths.get(path)).collect(Collectors.toList[String])
    var lines = context.textFile(path).collect().toSeq
    val vocabsize = lines(0).toInt
    lines = lines.slice(1, lines.size)

    System.out.println("Loading vocab...")
    val lookupIndex = new Object2IntOpenHashMap[String]
    lookupIndex.defaultReturnValue(-1)

    var i = 0
    while (i < lines.size) {
      lookupIndex.put(lines(i), i)
      i += 1
    }

    lookupIndex.defaultReturnValue(lookupIndex.getInt(unknownWord))
    lookupIndex
  }

  /**
    * Load the vectors using memory mapping. Does not work over HDFS.
    * @param vocabsize the size of the vocabulary
    * @param path the path to the binary vectors
    * @return matrix of all word vectors
    */
  private def loadVectors(vocabsize: Int, path: String): Array[Array[Float]] = {
    val start = System.currentTimeMillis
    System.out.println("Loading vectors...")
    val ch = FileChannel.open(Paths.get(path), StandardOpenOption.READ)
    val totalsz = ch.size
    val dim = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    ch.read(dim, 0)
    dim.rewind
    val dims = dim.asIntBuffer.get
    println(s"expected dims $dims")

    val matrix : Array[Array[Float]] = Array.ofDim[Float](vocabsize, dims)

    var k = 0

    System.out.println("Start reading...")

    //Map 1M vectors at a time
    val sz : Long = (vocabsize / (1024 * 1024)) + 1
    var i : Long = 0
    while (i < sz) {
      System.out.println("Read " + i + "M vectors.")
      val startpos = 4 + i * 1024 * 1024 * dims * 4
      val endpos = Math.min(startpos + 1024 * 1024 * dims * 4, totalsz)
      val partsz = endpos - startpos
      val vecs = ch.map(FileChannel.MapMode.READ_ONLY, startpos, partsz).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer
      var left = partsz / dims / 4
      while (left > 0) {
        vecs.get(matrix(k))

        k += 1
        left -= 1
      }

      i += 1
    }

    val end = System.currentTimeMillis
    System.out.println("Done in " + (end - start) + " ms")
    System.out.println(String.format("Read %s vectors.", Integer.valueOf(matrix.length)))
    matrix
  }

}

/**
  * Created by marcusk on 2017-03-20.
  */
class Word2VecDict(vocab: Object2IntOpenHashMap[String], vecs:  Array[Array[Float]]) extends Serializable {
  def dim = vecs(0).length
  def numwords = vecs.length

  def vector(idx : Int) : Array[Float] = vecs(idx)
  def vectorIdx(str : String) : Int = {
    vocab.getInt(str)
  }
  def vector(str : String) : Array[Float] = vecs(vocab.getInt(str))
}


