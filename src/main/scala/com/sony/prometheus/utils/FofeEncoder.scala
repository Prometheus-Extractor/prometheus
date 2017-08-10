package com.sony.prometheus.utils

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.nd4j.linalg.factory.Nd4j

/**
  * Created by erik on 2017-08-10.
  */
class FofeEncoder(alpha: Double = 0.5, cacheSize: Int = 256) {

  val memory:Seq[Double] = (0 to cacheSize).map(Math.pow(alpha, _))

  /**
    * Encodes a sequence of vectors using FOFE.
    * The left most vector get the biggest weight and then it decreases.
    */
  def encodeRight(vectors: Seq[Vector]): Vector = {

    val fofeVector = vectors.map(v => Nd4j.create(v.toArray)).view.zipWithIndex.map{
      case (vector, index) =>
        val mod = if (index > cacheSize) Math.pow(alpha, index) else memory(index)
        vector.mul(mod)
    }.reduce((a, b) => a.add(b))

    Vectors.dense(fofeVector.data().asDouble())
  }

  def encodeLeft(vectors:Seq[Vector]): Vector = {
    encodeRight(vectors.reverse)
  }

}
