package dpla.ingestion3.executors

import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.machineLearning.{BagOfWordsTokenizer, Lemmatizer, TopicDistributor}
import dpla.ingestion3.messages._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import scala.util.{Failure, Success}

trait TopicModelExecutor extends Serializable with IngestMessageTemplates {

  /**
    * Performs the mapping for the given provider
    *
    * @param sparkConf Spark configurations
    * @param dataIn Path to harvested data
    * @param dataOut Path to save mapped data
    * @param shortName Provider short name
    * @param logger Logger to use
    */
  def executeTopicModel(sparkConf: SparkConf,
                        dataIn: String,
                        dataOut: String,
                        shortName: String,
                        stopWordsSource: String,
                        cvModelSource: String,
                        ldaModelSource: String,
                        logger: Logger): String = {

    // Fields to use for topic modeling
    val dataFields: Seq[String] = Seq(
      "SourceResource.title",
      "SourceResource.subject.providedLabel",
      "SourceResource.description"
    )

    val idField: String = "dplaUri"

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    // This start time is used to measure the duration of mapping.
    val startTime = System.currentTimeMillis()

    // Output helper
    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "topic_model", startDateTime)

    val outputPath = outputHelper.activityPath

    // Initialize spark
    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    logger.info("Starting machine learning.")

    // Initialize ML class instances
    val lemmatizer = new Lemmatizer(spark)
    val bagOfWordsTokenizer = new BagOfWordsTokenizer(stopWordsSource, spark)
    val topicDistributor= new TopicDistributor(cvModelSource, ldaModelSource, spark)

    // Read in enriched data
    val enriched = spark.read.avro(dataIn)

    logger.info("Lemmatizing.")

    val lemmas: DataFrame =
      lemmatizer.transform(df=enriched, idCol=idField, inputCols=dataFields, outputCol="lemmas")

    val bagOfWords: DataFrame =
      bagOfWordsTokenizer.transform(df=lemmas, inputCol="lemmas", outputCol="bagOfWords")

    val topicDistributions: DataFrame =
      topicDistributor.transform(df=bagOfWords, inputCol="bagOfWords", outputCol="topicDist")

    topicDistributions
      .select(idField, "lemmas", "bagOfWords", "topicDist")
      .write
      .parquet(outputPath + "/topicDistributions")

    // Write out topic distributions
    topicDistributions.write.parquet(outputPath + "/topicDistributions")

    val endTime: Double = System.currentTimeMillis()
    val runTime: Double = endTime-startTime

    // Write manifest
    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Machine Learning",
      "Provider" -> shortName,
      "Record count" -> enriched.count.toString,
      "Input" -> dataIn,
      "Runtime" -> runTime.toString
    )
    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    logger.info("Machine learning complete.")
    logger.info("Runtime: " + runTime.toString)

    spark.stop()

    // Return output destination of mapped records
    outputPath
  }
}
