package com.sony.prometheus.utils


import java.nio.file.{Files, Paths}
import org.apache.hadoop.fs.{Path}
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
  import Colours._

  def colouredExistString(fileType: String, file: String, exists: Boolean): String = {
    if (exists)
      s"${GREEN}${fileType} ${file} exists${RESET}"
    else
      s"${RED}${fileType} ${file} is not available${RESET}"
  }

  /** Returns true if path (file or dir) exists
    * @param path - the path to check, hdfs or local
    * @return     - true if path exists
    */
  def pathExists(path: String)(implicit sc: SparkContext): Boolean = {
    path.split(":") match {
      case Array("hdfs", suffix) => {
        val conf = sc.hadoopConfiguration
        envOrNone("HDFS_ADDRESS").foreach(fsName =>
          conf.set("fs.default.name", fsName)
        )
        val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
        val exists = fs.exists(new Path(suffix))
        println(colouredExistString("HDFS file", suffix, exists))
        exists
      }
      case Array("file", suffix) => {
        val exists = Files.exists(Paths.get(suffix))
        println(colouredExistString("Local file", suffix, exists))
        exists
      }
      case Array("s3", suffix) => {
        val conf = sc.hadoopConfiguration
        conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("fs.default.name", "s3://sony-prometheus-data")
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        val exists = fs.exists(new Path(suffix))
        println(colouredExistString("s3 file", suffix, exists))
        exists
      }
      case _ => {
        System.err.println(s"Illegal pattern for $path, specify path prefix (hdfs:, file:, or s3:)")
        true
      }
    }
  }
}
