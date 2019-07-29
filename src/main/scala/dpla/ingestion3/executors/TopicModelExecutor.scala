package dpla.ingestion3.executors

import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.machineLearning.TopicModelDriver
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

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    // This start time is used to measure the duration of mapping.
    val startTime = System.currentTimeMillis()

    logger.info("Starting machine learning.")

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "topic_model", startDateTime)

    val outputPath = outputHelper.activityPath

    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    val enrichedRecords = spark.read.avro(dataIn)

    val topicModelDriver = new TopicModelDriver(stopWordsSource, cvModelSource, ldaModelSource, spark)

    val topicDistributions: DataFrame = topicModelDriver.execute(enrichedRecords)

    // Write out learned intelligence
    topicDistributions.write.parquet(outputPath + "/topicDistributions") // TODO add path to output helper

    // Write manifest
    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Machine Learning",
      "Provider" -> shortName,
      "Record count" -> enrichedRecords.count.toString,
      "Input" -> dataIn
    )
    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    val endTime: Double = System.currentTimeMillis()
    val runTime: Double = endTime-startTime

    logger.info("Machine learning complete.")
    logger.info("Runtime: " + runTime.toString)

    spark.stop()

    // Return output destination of mapped records
    outputPath
  }
}
