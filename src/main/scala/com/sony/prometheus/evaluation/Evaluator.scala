package com.sony.prometheus.evaluation

import java.io.BufferedOutputStream

import com.sony.prometheus.annotators.VildeAnnotater
import com.sony.prometheus.Prometheus
import com.sony.prometheus.stages.{Predictor, PredictorStage, _}
import com.sony.prometheus.utils.{Coref, Utils}
import com.sony.prometheus.utils.Utils.pathExists
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import se.lth.cs.docforia.Document
import se.lth.cs.docforia.graph.disambig.NamedEntityDisambiguation
import se.lth.cs.docforia.memstore.{MemoryDocument, MemoryDocumentIO}

import scala.collection.JavaConverters._

/** Pipeline stage to run evaluation
 *
 * @param path           path to save evaluation results
 * @param evaluationData the Data to evaluate, should point to path with
 *                       [[ModelEvaluationDataPoint]]:s
 */
class ModelEvaluatorStage(
  path: String,
  evaluationData: ModelEvaluationData,
  lang: String,
  predictor: Predictor,
  nBest: Option[Int])(implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val evalDataPoints: RDD[ModelEvaluationDataPoint] = ModelEvaluationDataReader.load(evaluationData.getData())
      .filter(dP => dP.wd_sub != "false" && dP.wd_obj != "false")
      .filter(dP => dP.positive())  // Don't use the "incorrect annotation by the google algorithm"
    val annotatedEvidence = Evaluator.annotateTestData(evalDataPoints, path, lang)
    val evaluation = Evaluator.evaluateModel(evalDataPoints, annotatedEvidence, predictor, Some(path + "_debug.tsv"), nBest)

    Evaluator.save(evaluation.toString, path)
  }
}

class DataEvaluationStage(
   path: String,
   entityPairs: EntityPairExtractorStage,
   lang: String,
   predictions: PredictorStage,
   nBest: Option[Int])(implicit sqlContext: SQLContext, sc: SparkContext) extends Task with Data {

  override def getData(): String = {
    if (!pathExists(path)) {
      run()
    }
    path
  }

  override def run(): Unit = {
    val knownRelations = EntityPairExtractor.load(entityPairs.getData())
    val extractions = Predictor.load(predictions.getData())
    val results = Evaluator.evaluateData(knownRelations, extractions, nBest)
    val header = "probability threshold\tname\tnumber of extractions\tfound percentage\tverified percentage\tprecision for n most probable"
    Evaluator.save(header + results.mkString("\n"), path)
  }
}

/** Performs evaluation of [[ModelEvaluationDataPoint]]:s and/or extractions
  */
object Evaluator {
  val log = LogManager.getLogger(Evaluator.getClass)


  /** Annotate the snippets data with VildeAnnotater, use cache if possible
    *
    * @param evalDataPoints  [[ModelEvaluationDataPoint]]:s to annotate
    * @param path            path to the cache file
    * @return                RDD of annotateted Documents
    */
  def annotateTestData(evalDataPoints: RDD[ModelEvaluationDataPoint], path: String, lang: String)
                      (implicit sqlContext: SQLContext, sc: SparkContext): RDD[Document] = {
    import sqlContext.implicits._

    val file = path.split("-").last
    val l = path.split("/").length
    val cachePath = path.split("/").slice(0, l - 1).mkString("/") + "/cache/" + file + ".cache"
    log.info(s"Caching $file to $cachePath")

    if (Utils.pathExists(cachePath)) {
      log.info(s"Using cached Vilde-annotated $file")
      val df = sqlContext.read.parquet(cachePath)
      df.map(row => {
        val d = MemoryDocumentIO.getInstance().fromBytes(row.getAs(0): Array[Byte]): Document
        if (Prometheus.conf.corefs()) {
          Coref.propagateCorefs(d)
        }
        d
      })
    } else {
      log.info(s"Did not find cached $file, annotating with Vilde...")
      val annotatedEvidence =
        evalDataPoints
          // treat multiple snippets as one string of multiple paragraphs
          .map(dP => dP.evidences.map(_.snippet).mkString("\n"))
          .map{e =>
            val docEither = VildeAnnotater.annotate(e, lang = lang, conf = "herd")
            docEither match {
              case Right(doc) => doc
              case Left(msg) => {
                log.error(msg)
                new MemoryDocument()
              }
            }
          }
      annotatedEvidence
        .map(doc => Tuple1(doc.toBytes))
        .toDF("doc")
        .write.parquet(cachePath)
      annotatedEvidence
    }
  }

  /**
    * Evaluate *extractions*      (from e.g. Wikipedia) against *knownRelations* (from e.g. Wikidata)
    * @param knownRelations       the entities that are known to partake in certain relations (ground truth)
    * @param extractions          extractions from e.g. Wikipedia
    * @param nMostProbable        optionally evaluate against nMostProbable extractions only
    *
    * @return The result of evaluation for each relation
    */
  def evaluateData(knownRelations: RDD[Relation], extractions: RDD[ExtractedRelation], nMostProbable: Option[Int])
                  (implicit sqlContext: SQLContext, sc: SparkContext): Seq[RelationEvaluationResult] = {

    val formatter = java.text.NumberFormat.getIntegerInstance

    // Cache some RDDs
    extractions.cache()
    knownRelations.cache()
    val modelThreshold = extractions.map(_.probability).min()

    log.info(s"Relation model threshold: $modelThreshold")

    val totalExtractions = extractions.filter(_.probability >= modelThreshold).count()
    val totalKnownRelations = knownRelations.count()

    log.info(s"Total known relations ${formatter.format(totalKnownRelations)}")
    log.info(s"Total extracted relations: ${formatter.format(totalExtractions)}")

    val relationTypes = knownRelations.map(r => (r.id, r.name)).collect()

    // Evaluate relation by relation
    val results = relationTypes.flatMap{ case (id, name) => {
      log.info(s"----- Evaluating data for $name... -------")

      // Key the extractions for this relation by subject-predicate
      val keyedExtractions = extractions
        .filter(_.probability >= modelThreshold)
        .filter(_.predictedPredicate == id)
        .map(e => (s"${e.subject}${e.predictedPredicate}", e))
        .groupByKey()

      keyedExtractions.cache()

      val nbrExtractions = keyedExtractions.flatMap{case (_, es) => es}.count()
      log.info(s"There are ${formatter.format(nbrExtractions)} extractions for $name")

      // Key known relations for this by subject-predicate
      val keyedValidations = knownRelations
        .filter(_.id == id)
        .flatMap(rel =>
          rel.entities.map(entPair => {
            (s"${entPair.source}${rel.id}", entPair)
          })
        ).groupByKey()

      keyedValidations.cache()
      val nbrValidations = keyedValidations.flatMap{case (_, ks) => ks}.count()
      log.info(s"There are ${formatter.format(nbrValidations)} known $name triples in Wikidata")

      // Join extractions and validations to get common pairs (matching subj and predicate but not necessarily obj)
      val pairMatches = keyedExtractions.join(keyedValidations)
      // Nbr of found subj-predicate pairs that matches corresponding Wikidata known relation
      val nbrPairMatches = keyedExtractions.count()
      // Total nbr of found triples that at least partially matches entries in Wikidata
      val nbrTotalMatches = pairMatches.flatMap{case (_, (es, _)) => es}.count()
      log.info(s"There are ${formatter.format(nbrTotalMatches)} matching subject-predicate pairs for $name")

      // Filter out correct pairMatches (where also the object is correct)
      val verifiedCorrect = pairMatches.flatMap{case (_, (es, pairs)) => {
        es.filter(extraction => pairs.exists(pair => pair.dest == extraction.obj))
      }}

      val nbrVerified = verifiedCorrect.count()
      log.info(s"There are ${formatter.format(nbrVerified)} verified triples for $name")

      val conflictingMatches = pairMatches.flatMap{case (_, (es, pairs)) => {
        es.filter(extraction => !pairs.exists(pair => pair.dest == extraction.obj))
      }}

      val nbrConflicting = conflictingMatches.count()
      log.info(s"There are ${formatter.format(nbrConflicting)} conflicting pairMatches for $name")

      // Proportion of Wikidata entries that we find at least a partial match
      val foundProportion: Double =  nbrPairMatches / nbrExtractions.toDouble
      val foundPercentage = f"${foundProportion * 100}%4.2f"
      val verifiedProportion: Double =  nbrVerified / nbrTotalMatches.toDouble
      val verifiedPercentage = f"${verifiedProportion * 100}%4.2f"
      log.info(s"Found suggestions for $foundPercentage% of the extractions")
      log.info(s"Correctly verified $verifiedPercentage% of the matching extractions")

      val theseMatches = pairMatches
        .flatMap{case (_, (es, _)) => es}
        .filter(_.probability >= modelThreshold)

      val nMostPrecision = nMostProbable.map(n => {
        val predictions = theseMatches.map(Seq(_))
        val (precision, _, _) = nMostProbableOutcome(predictions, verifiedCorrect, conflictingMatches, nbrValidations, n)
        log.info(s"$n most probable precision: $precision")
        precision
      })

      // Evaluate with different thresholds; from model's up to end
      val end = 1.0
      val step = (end - modelThreshold) / 20

      val theseResults = (modelThreshold + step to end by step).map(threshold => {
        log.info(s"Computing outcome if probability threshold was: ${f"$threshold%4.2f"}...")
        val outcome = outcomeByThreshold(name, threshold, theseMatches, verifiedCorrect, nbrValidations.toInt)
        log.info(s"\tVerified percentage: ${f"${outcome.verifiedPercentage}%4.2f"}")
        println(outcome)
        outcome
      })

      Seq(
        Seq(RelationEvaluationResult(modelThreshold, name, nbrExtractions.toInt, foundProportion, verifiedProportion, nMostPrecision)),
        theseResults
      ).flatten
    }}
    results
  }

  private def outcomeByThreshold(name: String, threshold: Double, extractions: RDD[ExtractedRelation],
                                 verifiedExtractions: RDD[ExtractedRelation], nbrRelations: Int): RelationEvaluationResult = {

    val newExtractions = extractions.filter(_.probability >= threshold)
    val nbrExtractions = newExtractions.count()
    val newFoundPercentage: Double = nbrExtractions / nbrRelations.toDouble
    val newVerifiedExtractions = verifiedExtractions.filter(_.probability >= threshold)
    val newVerifiedPercentage: Double = newVerifiedExtractions.count() /  nbrRelations.toDouble

    RelationEvaluationResult(threshold, name, nbrExtractions.toInt, newFoundPercentage, newVerifiedPercentage, None)
  }

  case class RelationEvaluationResult(
    probabilityThreshold: Double,
    name: String,
    nbrExtractions: Int,
    foundPercentage: Double,
    verifiedPercentage: Double,
    nMostPrecision: Option[Double]) {

    override def toString(): String = {
      val s = s"$probabilityThreshold\t$name\t$nbrExtractions\t$foundPercentage\t$verifiedPercentage"
      if (nMostPrecision.isDefined) {
        s + s"\t${nMostPrecision.get}"
      } else {
        s
      }
    }
  }

  /** Returns an [[EvaluationResult]]
 *
    * @param evalDataPoints RDD of [[ModelEvaluationDataPoint]]
    * @return                 an [[EvaluationResult]]
   */
  def evaluateModel(evalDataPoints: RDD[ModelEvaluationDataPoint], annotatedEvidence: RDD[Document], predictor: Predictor,
                    debugOutFile: Option[String], nMostProbable: Option[Int])
                   (implicit sqlContext: SQLContext, sc: SparkContext): EvaluationResult = {

    evalDataPoints.cache()
    annotatedEvidence.cache()

    val herdPoints = evalDataPoints.zip(annotatedEvidence).filter(herdSucceeded)

    val nbrEvalDataPoints = evalDataPoints.count()
    val nbrTrueDataPoints = evalDataPoints.filter(_.positive()).count()
    val nbrNegDataPoints = nbrEvalDataPoints - nbrTrueDataPoints

    log.info(s"There are ${nbrTrueDataPoints.toInt} positive examples in the evaluation data")
    log.info(s"There are $nbrNegDataPoints negative examples in the evaluation data")

    val herdRecall = herdPoints.count() / nbrEvalDataPoints.toDouble
    log.info(s"Herd successfully found ${herdPoints.count()} target entity pairs, and recall is about: $herdRecall")

    log.info("Testing predictor on test set")
    val predictedRelations = predictor.extractRelations(annotatedEvidence)
      .map(rs => rs.filter(r => !r.predictedPredicate.contains(predictor.UNKNOWN_CLASS)))
      .map(rs => {
        // Distinct the predictions for each data point. That is all triples are unique per data point.
        rs.groupBy(r => s"${r.subject}${r.predictedPredicate}${r.obj}").map{
          case (key: String, relations: Seq[ExtractedRelation]) =>
            relations.sortBy(1 - _.probability).head
        }.toSeq
      }).cache()

    val zippedTestPrediction = evalDataPoints.zip(predictedRelations).cache()

    val nbrPredictedRelations = predictedRelations.map(_.length).reduce(_ + _)
    log.info(s"Extracted $nbrPredictedRelations relations from evaluation data")

    log.info("Evaluating the predicted relations")
    val truePositives: RDD[ExtractedRelation] = zippedTestPrediction.flatMap{
      case (datapoint, predRelations) =>
        predRelations.filter(r => isCorrectPrediction(datapoint, r ))
    }.cache()

    val falsePositives: RDD[ExtractedRelation] = zippedTestPrediction.flatMap{
      case (datapoint, predRelations) =>
        predRelations.filter(r => !isCorrectPrediction(datapoint, r))
    }.cache()

    val nbrTruePositives = truePositives.count()
    val nbrFalsePositives = falsePositives.count()

    log.info(s"True Positives: $nbrTruePositives")
    log.info(s"False Positives: $nbrFalsePositives")

    val recall: Double = nbrTruePositives / nbrTrueDataPoints.toDouble
    val precision: Double = nbrTruePositives / nbrPredictedRelations.toDouble
    val f1: Double = computeF1(recall, precision)

    log.info(s"Precision is $precision")
    log.info(s"Recall is $recall")
    log.info(s"F1 is $f1")

    nMostProbable.foreach(N =>
      nMostProbableOutcome(predictedRelations, truePositives, falsePositives, nbrTrueDataPoints, N)
    )
    suggestThreshold(truePositives, falsePositives, nbrTrueDataPoints)

    debugOutFile.foreach(file => {
      val data = evalDataPoints.zip(annotatedEvidence).zip(predictedRelations).map{
        case ((evalPoint, annotatedDocument), relations) =>
          val herdResult = herdSucceeded(evalPoint, annotatedDocument)
          val relResults = relations.map(r => s"${r.subject}/${r.predictedPredicate}/${r.obj} - ${r.probability} - ${isCorrectPrediction(evalPoint, r)}").mkString("\t")
          s"${evalPoint.evidences.map(_.snippet).mkString(" >> ")}\t${evalPoint.positive()}\t${evalPoint.wd_sub}/${evalPoint.wd_pred}/${evalPoint.wd_obj}\t$herdResult\t$relResults"
      }.collect().mkString("\n")
      val header = "Sentences\tPositive Datapoint\tRDF-triple\tHerd\tPredicted Results:\n"
      writeDebugFile(header, data, file)
    })

    val evaluation: EvaluationResult = EvaluationResult(
      evalDataPoints.first().wd_pred,
      nbrTrueDataPoints.toInt,
      nbrTruePositives.toInt,
      recall,
      precision,
      f1,
      herdRecall)
    log.info(s"EvaluationResult: $evaluation")
    evaluation
  }

  private def herdSucceeded(zippedPoint: (ModelEvaluationDataPoint, Document)): Boolean = {
    val dataPoint = zippedPoint._1
    val evidence = zippedPoint._2
    val ents = evidence.nodes(classOf[NamedEntityDisambiguation]).asScala.toSeq.map(_.getIdentifier.split(":").last)
    ents.contains(dataPoint.wd_obj) && ents.contains(dataPoint.wd_sub)
  }

  private def isCorrectPrediction(datapoint: ModelEvaluationDataPoint, r: ExtractedRelation): Boolean = {
    datapoint.wd_obj == r.obj && datapoint.wd_sub == r.subject && datapoint.wd_pred == r.predictedPredicate
  }

  private def computeF1(recall: Double, precision: Double): Double = 2 * (precision * recall) / (precision + recall)

  private def writeDebugFile(header: String, data: String, file: String)(implicit sc: SparkContext): Unit = {
    // Save debug information to TSV
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val output = fs.create(new Path(file))
    val os = new BufferedOutputStream(output)
    log.info(s"Saving debug information to $file...")
    os.write(header.getBytes("UTF-8"))
    os.write(data.getBytes("UTF-8"))
    os.close()
  }

  private def nMostProbableOutcome(predictedRelations: RDD[Seq[ExtractedRelation]],
                                   truePositives: RDD[ExtractedRelation],
                                   falsePositives: RDD[ExtractedRelation],
                                   nbrTrueDataPoints: Double,
                                   n: Int): (Double, Double, Double) = {
    log.info(s"Evaluating for $n most probable...")
    val cutOff = predictedRelations
      .flatMap(rs => rs.map(_.probability))
      .takeOrdered(n)(math.Ordering.Double.reverse)
      .last
    log.info(s"cutOff is: $cutOff")
    val trueProb = truePositives.map(_.probability).collect()
    val falseProb = falsePositives.map(_.probability).collect()
    val newTP = trueProb.count(_ >= cutOff)
    val newFP = falseProb.count(_ >= cutOff)
    val recall = newTP / nbrTrueDataPoints
    val precision = newTP / (newTP + newFP).toDouble
    val f1 = computeF1(recall, precision)
    log.info(s"\tWith $n most probable => precision: $precision, recall: $recall, f1: $f1")
    (precision, recall, f1)
  }

  private def suggestThreshold(truePositives: RDD[ExtractedRelation], falsePositives: RDD[ExtractedRelation],
                               nbrTrueDataPoints: Double): Unit = {

    val tpCount = truePositives.count().toDouble
    val fpCount = falsePositives.count().toDouble
    val trueProb = truePositives.map(_.probability).collect()
    val falseProb = falsePositives.map(_.probability).collect()
    val meanProbTP = trueProb.sum / tpCount
    val meanProbFP = falseProb.sum / fpCount
    log.info(s"Predictor mean probabilities for TP: $meanProbTP, FP: $meanProbFP")

    if (meanProbFP < meanProbTP) {
      for (cutoff <- meanProbFP to 1.0 by (meanProbTP - meanProbFP) / 20) {
        val newTP = trueProb.count(_ >= cutoff)
        val newFP = falseProb.count(_ >= cutoff)

        val recall = newTP / nbrTrueDataPoints
        val precision = newTP / (newTP + newFP).toDouble
        val f1 = computeF1(recall, precision)
        log.info(s"\tWith probability cutoff $cutoff => recall: $recall, precision: $precision, f1: $f1")
      }
    }
  }

  def save(data: String, path: String)(implicit sc: SparkContext): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val output = fs.create(new Path(path + ".tsv"))
    val os = new BufferedOutputStream(output)
    os.write(data.getBytes("UTF-8"))
    os.close()
  }

  case class EvaluationResult(
    relation: String,
    nbrDataPoints: Int,
    truePositives: Int,
    recall: Double,
    precision: Double,
    f1: Double,
    herdRecall: Double) {

    override def toString: String = {
      s"""relation\tnbr data points\ttrue positives\trecall\tprecision\tf1\tHERD recall
         |$relation\t$nbrDataPoints\t$truePositives\t$recall\t$precision\t$f1\t$herdRecall
      """.stripMargin
    }
  }



}
