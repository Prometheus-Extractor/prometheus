package com.sony.prometheus.pipeline

import java.nio.file.{Paths, Files}
import org.apache.spark.SparkContext

/** A runnable task in the Pipeline, implemented by e.g. [[com.sony.prometheus.FeatureExtractorStage]]
 */
trait Task {
  def run(): Unit
}

/** A task in the the pipeline that produces data, implemented by e.g. [[com.sony.prometheus.CorpusReader]]
 */
trait Data {
  def getData(): String
  def exists(path: String)(implicit sc: SparkContext): Boolean = {
    if (path.split(":")(0) == "hdfs") {
      val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      fs.exists(new org.apache.hadoop.fs.Path(path.split(":")(1)))
    } else {
      Files.exists(Paths.get(path))
    }
  }
}

