package se.lth.cs.nlp.entiforia

import java.io.{File, IOException}
import java.nio.{ByteBuffer, ByteOrder, FloatBuffer}
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.stream.Collectors

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

import scala.collection.mutable

/**
  * Created by marcusk on 2017-03-20.
  */
class Word2VecDict(vocabFile : File, vecsFile : File, unknownWord : String="__UNKNOWN__") {
  private val vocab : Object2IntOpenHashMap[String] = loadVocab(vocabFile.getAbsolutePath)
  private val vecs  : INDArray                      = loadVectors(vocab.size(), vecsFile.getAbsolutePath)

  /**
    * Load the vectors
    * @param vocabsize the size of the vocabulary
    * @param path the path to the binary vectors
    * @return matrix of all word vectors
    */
  private def loadVectors(vocabsize: Int, path: String): INDArray = {
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
    Nd4j.create(matrix)
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

  def dim = vecs.columns()
  def numwords = vecs.rows()

  def vector(idx : Int) : INDArray = vecs.getRow(idx)
  def vectorIdx(str : String) : Int = {
    vocab.getInt(str)
  }
  def vector(str : String) : INDArray = vecs.getRow(vocab.getInt(str))
}
