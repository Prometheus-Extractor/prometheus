package com.sony.prometheus.utils


import java.nio.file.{Files, Paths}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import scala.util.Properties.envOrNone

/**
  * Created by axel on 2017-03-23.
  */
object Utils {
  object Colours {
    final val RESET: String = "\u001B[0m"
    final val BOLD: String = "\u001B[1m"
    final val RED: String = "\u001B[31m"
    final val GREEN: String = "\u001B[32m"
  }


  /** Returns true if path (file or dir) exists
    * @param path - the path to check, hdfs or local
    * @return     - true if path exists
    */
  def pathExists(path: String)(implicit sc: SparkContext): Boolean = {
    if (path.split(":")(0) == "hdfs") {
      val conf = sc.hadoopConfiguration
      envOrNone("HDFS_ADDRESS").foreach(fsName =>
        conf.set("fs.default.name", fsName)
      )
      val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
      fs.exists(new Path(path.split(":")(1)))
    } else if (path.split(":")(0) == "s3") {
      val conf = sc.hadoopConfiguration
      conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      conf.set("fs.default.name", "s3://sony-prometheus-data")
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      fs.exists(new Path(path.split(":")(1)))
    } else {
      Files.exists(Paths.get(path))
    }
  }
}
