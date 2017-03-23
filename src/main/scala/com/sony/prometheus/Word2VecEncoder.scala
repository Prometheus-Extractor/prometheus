package com.sony.prometheus

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Sentence

/**
  * Created by erik on 2017-03-22.
  */
object Word2VecEncoder {

  val MAX_SENTENCE_LENGTH = 220

  def apply(docs: RDD[Document]): Word2VecEncoder = {

    val sentences = docs.map(doc => {
      doc.nodes(classOf[Sentence]).asScala.map(_.text()).filter(_.length <= MAX_SENTENCE_LENGTH).map(_.split(" ")).flatten
    })

   /* val t = new DefaultTokenizerFactory()
    t.setTokenPreProcessor(new CommonPreprocessor())

    val word2Vec = new Word2Vec.Builder()
      .tokenizerFactory(t).seed(42L).negative(3).useAdaGrad(false).layerSize(100).windowSize(5)
      .learningRate(0.025).minLearningRate(0.0001).iterations(1).batchSize(100).minWordFrequency(5)
      .useUnknown(true).build()

    word2Vec.train(sentences)
    */
    val word2vec = new Word2Vec().setVectorSize(400).setWindowSize(5).setMinCount(5).setSeed(42L)
    val model = word2vec.fit(sentences)
    new Word2VecEncoder(model)
  }

  def load(path: String, sparkContext: SparkContext): Word2VecEncoder = {
    new Word2VecEncoder(Word2VecModel.load(sparkContext, path))
  }

}

class Word2VecEncoder(model: Word2VecModel) extends Serializable{

  val VEC_SIZE = model.getVectors.values.head.length

  def index(token: String): Vector = {
    val vectors = model.getVectors
    vectors.get(token).map(ar => Vectors.dense(ar.map(_.toDouble))).getOrElse(Vectors.zeros(VEC_SIZE))
  }

  def save(path: String, sqlContext: SQLContext): Unit = {
    //WordVectorSerializer.writeWordVectors(model.lookupTable(), path)
    model.save(sqlContext.sparkContext, path)
  }
}

