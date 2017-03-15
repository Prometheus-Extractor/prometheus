package com.sony.prometheus

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.text.Token

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

/** Provides String indexer
 */
object TokenEncoder {

  val TOKEN_MIN_COUNT = 3

  def createWordEncoder(docs: RDD[Document]): TokenEncoder = {

    val tokens = docs.flatMap(doc => {
      doc.nodes(classOf[Token]).asScala.toSeq.map(t => t.text())
    })

    val wordTokens = tokens.filter(Filters.wordFilter)

    val normalizedTokens = wordTokens.map(normalize)

    val commonTokens = normalizedTokens.map(token => (token, 1))
      .reduceByKey(_ + _)
      .sortByKey(ascending=false)
      .filter(tup => tup._2 >= TOKEN_MIN_COUNT)
      .map(_._1)

    val zippedTokens: RDD[(String, Int)] = commonTokens.zipWithIndex().map(t=> (t._1, t._2.toInt + 1))
    createTokenEncoder(zippedTokens)
  }

  def createPosEncoder(docs: RDD[Document]): TokenEncoder = {

    val pos = docs.flatMap(doc => {
      doc.nodes(classOf[Token]).asScala.toSeq.map(_.getPartOfSpeech)
    }).distinct.zipWithIndex.map(p => (p._1, p._2.toInt + 1))

    createTokenEncoder(pos)

  }

  def normalize(token: String): String = {
    token.toLowerCase
  }

  def load(path: String, context: SparkContext): TokenEncoder = {
    val zippedTokens = context.objectFile[(String, Int)](path)
    createTokenEncoder(zippedTokens)
  }

  private def createTokenEncoder(zippedTokens: RDD[(String, Int)]): TokenEncoder = {
    val token2Id = new Object2IntOpenHashMap[String]()
    val id2Token = new Int2ObjectOpenHashMap[String]()
    zippedTokens.collect().foreach(t => {
      token2Id.put(t._1, t._2)
      id2Token.put(t._2, t._1)
    })

    new TokenEncoder(token2Id, id2Token)
  }

}

/** A String indexer that maps String:s to Int:s and back
  * Index 0 is always "unknown token"
 */
@SerialVersionUID(1)
class TokenEncoder(token2Id: Object2IntOpenHashMap[String], id2Token: Int2ObjectOpenHashMap[String]) extends java.io.Serializable{

  /** Gets the index of token
    *
    *  @param token   the String to map to Int
    *  @return        the Int that maps to the token or -1 if not found
    */
  def index(token: String): Int = {
    val t = TokenEncoder.normalize(token)
    token2Id.getOrDefault(t, 0)
  }

  /** Gets the String mapping to index
    *
    *   @param index    the index to map to String
    *   @return         the String that maps to index, or "<UNKNOWN_ID>" if not found
    */
  def token(index: Int): String = {
    id2Token.getOrDefault(index, "<UNKNOWN_ID>")
  }

  def vocabSize(): Int = {
    token2Id.size()
  }

  /** Saves the TokenEncoder to disk
   */
  def save(path: String, sqlContext: SQLContext) {
    val rdd = sqlContext.sparkContext.parallelize(token2Id.entrySet().asScala.map(e => (e.getKey, e.getValue)).toSeq)
    rdd.saveAsObjectFile(path)
  }

}
