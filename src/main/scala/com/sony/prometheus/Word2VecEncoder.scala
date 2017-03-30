package com.sony.prometheus

import java.io.File
import javax.annotation.RegEx

import com.sony.prometheus.pipeline.Data
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors
import org.deeplearning4j.spark.models.embeddings.word2vec.Word2Vec
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Sentence

class Word2VecData(path: String)(implicit sc: SparkContext) extends Data {

  override def getData(): String = {
    if(exists(path)) {
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

    val log = LogManager.getLogger(Word2VecEncoder.getClass)

    val startTime = System.currentTimeMillis()
    val model = WordVectorSerializer.readWord2VecModel(modelPath)
    log.info(s"Read binary word2vec model in ${(System.currentTimeMillis() - startTime)/1000} s")
    new Word2VecEncoder(model)

  }
}

class Word2VecEncoder(model: WordVectors) extends Serializable{

  val VEC_SIZE = model.lookupTable().vectors().next().length()

  def index(token: String): Vector = {
    if(model.hasWord(token) && !token.equals(FeatureExtractor.EMPTY_TOKEN))
      Vectors.dense(model.getWordVector(token))
    else
      Vectors.zeros(VEC_SIZE)
  }

}

