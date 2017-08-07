package com.sony.prometheus

/**
  * This package contains implementations for all of the stages in the pipeline.
  * [[com.sony.prometheus.stages.Task]] and [[com.sony.prometheus.stages.Data]] provide the abstractions for the
  * pipeline stages.
  *
  *==Notable Stages==
  * [[com.sony.prometheus.stages.CorpusReader]] - to read in training corpus
  *
  * [[com.sony.prometheus.stages.TrainingDataExtractorStage]] - to extract training data
  *
  * [[com.sony.prometheus.stages.FeatureExtractorStage]] - to extract features
  *
  * [[com.sony.prometheus.stages.FeatureTransformerStage]] - to encode features
  *
  * [[com.sony.prometheus.stages.FilterModelStage]] - to train a filter model
  *
  * [[com.sony.prometheus.stages.ClassificationModelStage]] - to train a classification model
  *
  * [[com.sony.prometheus.stages.PredictorStage]] - extract relations from a corpus
  */
package object stages {

}
