package com.sony.prometheus.utils

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkContext

/**
  * Created by axel on 2017-03-23.
  */
object Utils {

  /** Check if file exists, supports both local and hdfs storage
    *
    */
  def pathExists(path: String)(implicit sc: SparkContext): Boolean = {
    if (path.split(":")(0) == "hdfs") {
      val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      fs.exists(new org.apache.hadoop.fs.Path(path.split(":")(1)))
    } else {
      Files.exists(Paths.get(path))
    }
  }
}
