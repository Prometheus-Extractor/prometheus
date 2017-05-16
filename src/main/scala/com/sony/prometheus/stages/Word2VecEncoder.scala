package com.sony.prometheus.stages

import java.io.{Externalizable, File, ObjectInput, ObjectOutput}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.stream.Collectors

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.log4j.LogManager
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.sony.prometheus.utils.Utils.pathExists

class Word2VecData(path: String)(implicit sc: SparkContext) extends Data {

  var hasUploaded = false

  private def uploadFiles(): Unit = {
    val log = LogManager.getLogger(this.getClass)
    log.info(s"Uploading word2vec binary model to SparkFiles from $path")
    sc.addFile(path + "/model.opt.vocab")
    sc.addFile(path + "/model.opt.vecs")
    hasUploaded = true
  }

  private def getPaths(): String = {
    s"${SparkFiles.get("model.opt.vocab")}\n${SparkFiles.get("model.opt.vecs")}"
  }

  override def getData(): String = {
    if(pathExists(path)) {
      if(!hasUploaded){
        uploadFiles()
      }
      getPaths()
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

    val word2vec = new Word2VecEncoder()
    word2vec.vecName = "model.opt.vecs"
    word2vec.vocabName = "model.opt.vocab"
    word2vec
  }
}

/**
  * This class wraps the specific Word2Vec implementation. We've tried Spark's, DL4J's and now Marcus'.
  */
class Word2VecEncoder extends Externalizable{

  var model: Word2VecDict = null
  var vocabName: String = null
  var vecName: String = null

  /**
    * This method deserializes the file from the binary model file.
    */
  private def setup(): Unit = {

    if(model != null){
      return
    }

    this.synchronized {
      if(model != null){
        return
      }

      val log = LogManager.getLogger(Word2VecEncoder.getClass)
      log.info("Reading word2vec")
      val vocabFile = new File(SparkFiles.get(vocabName))
      val vecsFile = new File(SparkFiles.get(vecName))
      val startTime = System.currentTimeMillis()
      model = new Word2VecDict(vocabFile, vecsFile)
      log.info(s"Read binary word2vec model in ${(System.currentTimeMillis() - startTime)/1000} s")
    }
  }

  def index(token: String): Vector = {
    setup()
    val idx = model.vectorIdx(token)
    if(idx == -1){
      emptyVector
    }else{
      Vectors.dense(model.vector(idx).map(_.toDouble))
    }
  }

  def emptyVector: Vector = {
    Vectors.zeros(model.dim)
  }

  def vectorSize(): Int = {setup(); model.dim}

  override def writeExternal(out: ObjectOutput): Unit = {
    LogManager.getLogger(Word2VecEncoder.getClass).info("Serializing word2vec model")
    out.writeUTF(vocabName)
    out.writeUTF(vecName)
  }

  override def readExternal(in: ObjectInput): Unit = {
    LogManager.getLogger(Word2VecEncoder.getClass).info("Deserializing word2vec model")
    vocabName = in.readUTF()
    vecName = in.readUTF()
  }
}


/**
  * Created by marcusk on 2017-03-20.
  */
class Word2VecDict(vocabFile : File, vecsFile : File, unknownWord : String="__UNKNOWN__") extends Serializable {
  private val vocab : Object2IntOpenHashMap[String] = loadVocab(vocabFile.getAbsolutePath)
  private val vecs  : Array[Array[Float]] = loadVectors(vocab.size(), vecsFile.getAbsolutePath)

  /**
    * Load the vectors
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

    val matrix : Array[Array[Float]] = Array.ofDim[Float](vocabsize, dims)

    var k = 0

    System.out.println("Start reading...")

    // Map 1M vectors at a time
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

  /**
    * Load the vocabulary
    * @param path the path to the vocabulary
    * @return initilized vocabulary index
    */
  private def loadVocab(path : String) : Object2IntOpenHashMap[String] = {
    var lines = Files.lines(Paths.get(path)).collect(Collectors.toList[String])
    val vocabsize = lines.get(0).toInt
    lines = lines.subList(1, lines.size)

    System.out.println("Loading vocab...")
    val lookupIndex = new Object2IntOpenHashMap[String]
    lookupIndex.defaultReturnValue(-1)

    var i = 0
    while (i < lines.size) {
      lookupIndex.put(lines.get(i), i)
      i += 1
    }

    lookupIndex.defaultReturnValue(lookupIndex.getInt(unknownWord))
    lookupIndex
  }

  def dim = vecs(0).length
  def numwords = vecs.length

  def vector(idx : Int) : Array[Float] = vecs(idx)
  def vectorIdx(str : String) : Int = {
    vocab.getInt(str)
  }
  def vector(str : String) : Array[Float] = vecs(vocab.getInt(str))
}


