package com.sony.prometheus.stages

/** A runnable task in the Pipeline, implemented by eg [[FeatureExtractorStage]]
 */
trait Task {

  /** Runs the task, saving results to disk
   */
  def run(): Unit
}

/** A task in the pipeline that produces data, implemented by eg [[com.sony.prometheus.stages.CorpusReader]]
 */
trait Data {
  /** Returns the path to the Data if it already exists, otherwise if the class also
    * extends [[Task]], run() is called and the data is produced and saved to path, which
    * is returned.
    *
    * @return  - the path to the data
   */
  def getData(): String
}

