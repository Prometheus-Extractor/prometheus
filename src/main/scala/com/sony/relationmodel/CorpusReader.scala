package com.sony.relationmodel

import java.io.IOError

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{Accumulator, SparkContext}
import org.apache.spark.rdd.RDD
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.memstore.MemoryDocumentIO

object CorpusReader {
  def readCorpus(sqlContext: SQLContext, sc: SparkContext, file: String): RDD[Document] = {
    val log = LogManager.getRootLogger
    val df: DataFrame = sqlContext.read.parquet(file)
    log.debug(df.schema.mkString)

    val ioErrors: Accumulator[Int] = sc.accumulator(0, "IO_ERRORS")

    // we might need to filter for only articles here but that wouldn't be a generelized solution.
    val docs = df.flatMap{row =>
      try {
        val doc: Document = MemoryDocumentIO.getInstance().fromBytes(row.getAs(5): Array[Byte])
        List(doc)
      } catch {
        case e:IOError =>
          ioErrors.add(1)
          List()
      }
    }
    log.warn(s"$ioErrors IO Errors encountered")
    docs
  }
}
