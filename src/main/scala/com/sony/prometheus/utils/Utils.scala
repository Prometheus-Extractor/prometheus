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
        println(s"HDFS file $suffix $exists")
        exists
      }
      case Array("file", suffix) => {
        val exists = Files.exists(Paths.get(suffix))
        println(s"Local file $suffix $exists")
        exists
      }
      case _ => {
        System.err.println(s"Illegal pattern for $path, specify path prefix (hdfs: or file:)")
        true
      }
    }
  }
}
