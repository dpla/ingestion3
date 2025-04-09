package dpla.ingestion3.executors

import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.messages._
import dpla.ingestion3.model.OreAggregation
import dpla.ingestion3.profiles.CHProfile
import dpla.ingestion3.reports.summary._
import dpla.ingestion3.utils.{CHProviderRegistry, Utils}
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.json4s.JsonAST.JValue

import java.io.File
import java.time.LocalDateTime
import scala.reflect.io.Directory
import scala.util.{Failure, Success}
import scala.xml.NodeSeq

trait MappingExecutor extends Serializable with IngestMessageTemplates {

  private val logger = LogManager.getLogger(this.getClass)

  /** Lookup the profile and mapping class TODO this could be better (accepts
    * registry as param etc.)
    */
  private def getExtractorClass(
      shortName: String
  ): CHProfile[_ >: NodeSeq with JValue] =
    CHProviderRegistry.lookupProfile(shortName) match {
      case Success(extClass) => extClass
      case Failure(_) =>
        throw new RuntimeException(
          s"Unable to load $shortName mapping from CHProviderRegistry"
        )
    }

  /** Performs the mapping for the given provider
    *
    * @param sparkConf
    *   Spark configurations
    * @param dataIn
    *   Path to harvested data
    * @param dataOut
    *   Path to save mapped data
    * @param shortName
    *   Provider short name
    */
  def executeMapping(
      sparkConf: SparkConf,
      dataIn: String,
      dataOut: String,
      shortName: String
  ): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    // This start time is used to measure the duration of mapping.
    val startTime = System.currentTimeMillis()

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "mapping", startDateTime)

    val outputPath = outputHelper.activityPath

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    import spark.implicits._

    val dplaMap = new DplaMap()

    val tempLocation1 = s"$shortName-intermediate-results-1.parquet"
    val tempLocation2 = s"$shortName-intermediate-results-2.parquet"

    def deleteTempFiles(): Unit = {
      val tempDir1 = new Directory(new File(tempLocation1))
      val tempDir2 = new Directory(new File(tempLocation2))
      if (tempDir1.exists) tempDir1.deleteRecursively()
      if (tempDir2.exists) tempDir2.deleteRecursively()
    }

    deleteTempFiles()

    val harvestedRecords: DataFrame = spark.read.format("avro").load(dataIn)
    val attemptedCount: Long = harvestedRecords.count // evaluation

    // Run the mapping over the Dataframe
    // Transformation only
    val extractorClass = getExtractorClass(shortName) // lookup from registry

    val mappingResults = harvestedRecords
      .select("document")
      .as[String]
      .map(document => dplaMap.map(document, extractorClass))(
        ExpressionEncoder[OreAggregation]
      )

    // Save mapped results locally as parquet
    mappingResults.write.parquet(tempLocation1)
    mappingResults.unpersist(blocking = true)
    harvestedRecords.unpersist(blocking = true)

    val intermediateResults1 =
      spark.read.parquet(tempLocation1).as[OreAggregation]

    // Removes records from mappingResults that have at least one IngestMessage
    // with a level of IngestLogLevel.error
    // Transformation only
    val successResults = intermediateResults1
      .filter(record =>
        !record.messages.forall(_.level == IngestLogLevel.error)
      )

    // Results must be written before _LOGS.
    // Otherwise, spark interpret the `successResults' `outputPath' as
    // already existing, and will fail to write.
    successResults.write.format("avro").save(outputPath)

    intermediateResults1.unpersist(blocking = true)
    successResults.unpersist(blocking = true)

    // Get counts
    val validRecords = spark.read
      .format("avro")
      .load(outputPath)
    val validRecordCount = validRecords.count
    validRecords.unpersist(blocking = true)

    // Write manifest
    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Mapping",
      "Provider" -> shortName,
      "Record count" -> Utils.formatNumber(validRecordCount),
      "Input" -> dataIn
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    val endTime = System.currentTimeMillis()

    val logsPath = outputHelper.logsPath

    // Collect the values needed to generate the report
    val finalReport =
      buildFinalReport(
        spark.read.parquet(tempLocation1).as[OreAggregation],
        shortName,
        logsPath,
        startTime,
        endTime,
        attemptedCount,
        validRecordCount
      )(spark)

    // Format the summary report and write it log file
    val mappingSummary = MappingSummary.getSummary(finalReport)
    outputHelper.writeSummary(mappingSummary) match {
      case Success(s) => logger.info(s"Summary written to $s.")
      case Failure(f) => logger.warn(s"Summary failed to write: $f")
    }
    // Send the mapping summary to the console as well (for convenience)
    logger.info(mappingSummary)

    spark.stop()

    // Delete temporary files
    deleteTempFiles()

    // Return output destination of mapped records
    outputPath
  }

  /** Creates a summary report of the ingest by using the MappingSummary object
    *
    * @param results
    *   All attempted records
    * @param shortName
    *   Provider short name
    * @param logsBasePath
    *   Root directory to write to
    * @param startTime
    *   Start time of operation
    * @param endTime
    *   End time of operation
    * @param spark
    *   SparkSession
    * @return
    *   MappingSummaryData
    */
  private def buildFinalReport(
      results: Dataset[OreAggregation],
      shortName: String,
      logsBasePath: String,
      startTime: Long,
      endTime: Long,
      attemptedCount: Long,
      validRecordCount: Long
      // duplicateHarvestRecords: Long
  )(implicit spark: SparkSession): MappingSummaryData = {
    // Transformation
    val messages: DataFrame = MessageProcessor.getAllMessages(results)(spark)
    val warnings: DataFrame = MessageProcessor.getWarnings(messages)
    val errors: DataFrame = MessageProcessor.getErrors(messages)

    // These actions evaluate `messages', `warnings', and `errors'
    val warnCount: Long = warnings.count
    val errorCount: Long = errors.count

    val logFileList: List[(String, Dataset[Row])] = List(
      "errors" -> (errors, errorCount)
      // "warnings" -> (warnings, warnCount) //commenting this out to avoid data explosion
    ).filter { case (_, (_, count: Long)) => count > 0 } // drop empty
      .map { case (key: String, (data: Dataset[_], _: Long)) =>
        key -> data
      } // drop count

    // write out warnings and errors
    val logFileSeq: List[String] = logFileList.map {
      case (name: String, data: Dataset[Row]) =>
        val path = s"$logsBasePath/$name"
        Utils.writeLogsAsCsv(path, name, data, shortName)
        path
    }

    val recordErrorCount: Long = attemptedCount - validRecordCount
    val recordWarnCount: Long = MessageProcessor.getDistinctIdCount(warnings)

    val errorMsgDetails: String =
      MessageProcessor.getMessageFieldSummary(errors).mkString("\n")
    val warnMsgDetails: String =
      MessageProcessor.getMessageFieldSummary(warnings).mkString("\n")

    // time summary
    val timeSummary = TimeSummary(
      Utils.formatDateTime(startTime),
      Utils.formatDateTime(endTime),
      Utils.formatRuntime(endTime - startTime)
    )
    // operation summary
    val operationSummary = OperationSummary(
      attemptedCount,
      validRecordCount,
      recordErrorCount,
      logFileSeq
    )
    // messages summary
    val messageSummary = MessageSummary(
      errorCount,
      warnCount,
      recordErrorCount,
      recordWarnCount,
      errorMsgDetails,
      warnMsgDetails
    )

    MappingSummaryData(shortName, operationSummary, timeSummary, messageSummary)
  }
}

class DplaMap extends Serializable {

  /** Perform the mapping for a single record
    *
    * @param document
    *   The harvested record to map
    * @param extractorClass
    *   Mapping/extraction class defined in CHProfile
    *
    * @return
    *   An OreAggregation representing the mapping results (both success and
    *   failure)
    */
  def map(
      document: String,
      extractorClass: CHProfile[_ >: NodeSeq with JValue]
  ): OreAggregation = {
    extractorClass.mapOreAggregation(document)
  }
}
