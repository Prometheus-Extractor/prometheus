package com.sony.relationmodel

/**
  * Created by erik on 2017-02-16.
  */
object FeatureTransformer {

  def apply(): FeatureTransformer = {

  //    // Tokenization
  //    val wordPattern = Pattern.compile("\\p{L}{2,}|\\d{4}]")
  //
  //    val T = Token.`var`()
  //    val docsDF = docs.flatMap(doc => {
  //      doc.nodes(classOf[Token]).asScala.toSeq.map(t => t.text())
  //    }).filter(t => wordPattern.matcher(t).matches())
  //      .map(token => (token, 1))
  //      .reduceByKey(_+_)
  //      .filter(tup => tup._2 >= 3)
  //      .map(_._1)
  //      .toDF("tokens")
  //
  //    val indexer = new StringIndexer()
  //      .setInputCol("tokens")
  //      .setOutputCol("categoryIndex")
  //      .fit(docsDF)
  //    val indexed = indexer.transform(docsDF)
  //
  //    val encoder = new OneHotEncoder()
  //      .setInputCol("categoryIndex")
  //      .setOutputCol("categoryVec")
  //    val encoded = encoder.transform(indexed)
    new FeatureTransformer()
  }

}

class FeatureTransformer {

}
