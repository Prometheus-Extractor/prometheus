package com.sony.prometheus

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.deeplearning4j.spark.models.embeddings.word2vec.Word2Vec
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Sentence

/**
  * Created by erik on 2017-03-22.
  */
object Word2VecEncoder {

  val MAX_SENTENCE_LENGTH = 200

  def apply(docs: RDD[Document]): Word2VecEncoder = {

    val sentences = docs.flatMap(doc => {
      doc.nodes(classOf[Sentence]).asScala.map(_.text()).filter(_.length <= MAX_SENTENCE_LENGTH)
    })

    val t = new DefaultTokenizerFactory()
    t.setTokenPreProcessor(new CommonPreprocessor())

    val word2Vec = new Word2Vec.Builder()
      .tokenizerFactory(t).seed(42L).negative(3).useAdaGrad(false).layerSize(100).windowSize(5)
      .learningRate(0.025).minLearningRate(0.0001).iterations(1).batchSize(100).minWordFrequency(5)
      .useUnknown(true).build()

    word2Vec.train(sentences)
    new Word2VecEncoder(word2Vec)
  }

}

class Word2VecEncoder(model: Word2Vec) extends Serializable{
  def index(token: String): Array[Double] = {
    model.getWordVector(token)
  }

  def token(vec: Array[Double]): String = {
    ???
  }

  def vocabSize(): Int = {
    model.getConfiguration.getVocabSize
  }

  def save(path: String, sqlContext: SQLContext): Unit = {
  }
}
