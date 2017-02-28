package com.sony.prometheus

import java.nio.file.{Paths, Files}
import org.apache.spark.SparkContext

trait Task {
  def run(): Unit
}

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

