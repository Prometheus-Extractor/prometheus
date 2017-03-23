package com.sony.prometheus.pipeline

import java.nio.file.{Files, Paths}

import com.sony.prometheus.utils.Utils
import org.apache.spark.SparkContext

/** A runnable task in the Pipeline, implemented by eg [[com.sony.prometheus.FeatureExtractorStage]]
 */
trait Task {

  /** Runs the task, saving results to disk
   */
  def run(): Unit
}

/** A task in the the pipeline that produces data, implemented by eg [[com.sony.prometheus.CorpusReader]]
 */
trait Data {
  /** Returns the path to the Data if it already exists, otherwise if the class also
    * extends [[Task]], run() is called and the data is produced and saved to path, which
    * is returned.
    *
    * @returns  - the path to the data
   */
  def getData(): String

  /** Returns true if data is available in path
   * @param path - the path to check
   */
  def exists(path: String)(implicit sc: SparkContext): Boolean = Utils.pathExists(path)

}

