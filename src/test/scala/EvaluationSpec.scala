import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import com.sony.prometheus._
import com.sony.prometheus.evaluation._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import se.lth.cs.docforia.Document

class EvaluationSpec extends FlatSpec with BeforeAndAfter with Matchers with SharedSparkContext   {
  "EvaluationDataReader" should "read json file properly" in {
    implicit val sqlContext = new SQLContext(sc)
    val edPointsRDD: RDD[EvaluationDataPoint] = EvaluationDataReader.load("./src/test/data/evaluationTest.txt")
    val edPoints = edPointsRDD.collect()
    edPoints.head.wd_sub should equal ("Q3388789")
    edPoints.head.wd_obj should equal ("Q60")
    edPoints.head.wd_pred should equal ("P19")
    edPoints.head.obj should equal ("/m/02_286")
    edPoints.head.sub should equal ("/m/026_tl9")
    val j0 = edPoints.head.judgments.head
    j0.judgment should equal ("yes")
    j0.rater should equal ("11595942516201422884")
    val j1 = edPoints.head.judgments(1)
    j1.judgment should equal ("yes")
    j1.rater should equal ("16169597761094238409")
    val e0 = edPoints.head.evidences.head
    e0.url should equal ("http://en.wikipedia.org/wiki/Morris_S._Miller")
  }

  "EvaluationDataReader" should "extract snippets into annotated docs" in {
    implicit val sqlContext = new SQLContext(sc)
    val docs: RDD[Document] = EvaluationDataReader.getAnnotatedDocs("./src/test/data/evaluationTest.txt")
    val expected = """Morris Smith Miller (July 31, 1779 -- November 16, 1824) was
    |a United States Representative from New York. Born in New York City, he
    |graduated from Union College in Schenectady in 1798. He studied law and was
    |admitted to the bar. Miller served as private secretary to Governor Jay, and
    |subsequently, in 1806, commenced the practice of his profession in Utica. He was
    |president of the village of Utica in 1808 and judge of the court of common
    |pleas of Oneida County from 1810 until his death.""".stripMargin.replaceAll("\n", " ")

    docs.collect().mkString should equal (expected)
  }

  "Evaluator" should "evaluate" in {
    val relationModelPath = new File("../data/0.1.0-27-gc3d511d/relation_model/en")
    val entitiesFile = new File("../data/0.1.0-27-gc3d511d/entities")
    val corpusPath = new File("../data/wikipedia-corpus-herd")
    val evalFile = new File("../data/0.1.0-27-gc3d511d/relation_model/sv/eval_files/date_of_birth.json.txt")

    // First check that the required files are present, otherwise the test will take a long time
    relationModelPath should exist
    entitiesFile should exist
    corpusPath should exist
    evalFile should exist

    // Run the pipeline
    implicit val sqlContext = new SQLContext(sc)
    val corpusData = new CorpusData(corpusPath.getPath())(sc)
    val relationsData = new RelationsData(entitiesFile.getPath())(sc)
    val trainingTask = new TrainingDataExtractorStage(
      relationModelPath.getPath() + "/training_sentences",
      corpusData,
      relationsData)(sqlContext, sc)
    val featureTransformerTask = new FeatureTransformerStage(
      relationModelPath.getPath() + "/feature_model",
      corpusData)(sqlContext, sc)
    val featureExtractionTask = new FeatureExtractorStage(
      relationModelPath.getPath() + "/features",
      featureTransformerTask,
      trainingTask)(sqlContext, sc)
    val modelTrainingTask = new RelationModelStage(
      relationModelPath.getPath() + "/model",
      featureExtractionTask,
      featureTransformerTask,
      relationsData)(sqlContext, sc)

    val modelPath = new File(modelTrainingTask.getData())
    modelPath should exist

    val predictor = Predictor(modelTrainingTask, featureTransformerTask, relationsData)
    val evalDataPoints = EvaluationDataReader.load(evalFile.getPath())
    Evaluator.evaluate(evalDataPoints, predictor)(sqlContext, sc)
  }
}

