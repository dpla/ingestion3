package dpla.ingestion3.executors

import java.io.File
import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.messages._
import dpla.ingestion3.model
import dpla.ingestion3.model.RowConverter
import dpla.ingestion3.reports.summary._
import dpla.ingestion3.utils.{OutputHelper, ProviderRegistry, Utils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.util.{Failure, Success}

trait MappingExecutor extends Serializable {

  /**
    * Performs the mapping for the given provider
    *
    * @param sparkConf Spark configurations
    * @param dataIn Path to harvested data
    * @param dataOut Path to save mapped data
    * @param shortName Provider short name
    * @param logger Logger to use
    */
  def executeMapping( sparkConf: SparkConf,
                      dataIn: String,
                      dataOut: String,
                      shortName: String,
                      logger: Logger): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    // This start time is used to measure the duration of mapping.
    val startTime = System.currentTimeMillis()

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "mapping", startDateTime)

    val outputPath = outputHelper.outputPath

    // @michael Any issues with making SparkSession implicit?
    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    val sc = spark.sparkContext
    // TODO: assign checkpoint directory based on a configurable setting.

    // TODO Is it faster easier to use a counter than query a DF in most cases?
    val totalCount: LongAccumulator = sc.longAccumulator("Total Record Count")
    val successCount: LongAccumulator = sc.longAccumulator("Successful Record Count")
    val failureCount: LongAccumulator = sc.longAccumulator("Failed Record Count")

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    // these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    val oreAggregationEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    // Load the harvested record dataframe
    val harvestedRecords: DataFrame = spark.read.avro(dataIn).repartition(1024)

    // Run the mapping over the Dataframe
    val documents: Dataset[String] = harvestedRecords.select("document").as[String]

    val dplaMap = new DplaMap()

    // All attempted records
    val mappingResults: Dataset[Row] =
      documents.map(document =>
        dplaMap.map(document, shortName,
          totalCount, successCount, failureCount)
      )(oreAggregationEncoder)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Removes records from mappingResults that have at least one IngestMessage
    // with a level of IngestLogLevel.error
    val successResults: Dataset[Row] = mappingResults
      .filter(oreAggRow => {
        !oreAggRow // not
          .getAs[mutable.WrappedArray[Row]]("messages") // get all messages
          .map(msg => msg.getString(1)) // extract the levels into a list
          .contains(IngestLogLevel.error) // does that list contain any errors?
      })

    // Results must be written before _LOGS.
    // Otherwise, spark interpret the `successResults' `outputPath' as
    // already existing, and will fail to write.
    successResults.toDF().write.avro(outputPath)

    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Mapping",
      "Provider" -> shortName,
      "Record count" -> Utils.formatNumber(successResults.count),
      "Input" -> dataIn
    )
    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    val endTime = System.currentTimeMillis()

    val logsBasePath = outputHelper.logsBasePath

    // Collect the values needed to generate the report
    val finalReport = buildFinalReport(mappingResults, shortName, logsBasePath, startTime, endTime)(spark)

    // Format the summary report and write it log file
    // TODO Create MappingSummary.getEmailSummary() and
    logger.info(MappingSummary.getSummary(finalReport))

    spark.stop()

    // Return output destination of mapped records
    outputPath
  }

  /**
    * Creates a summary report of the ingest by using the MappingSummary object
    *
    * @param results All attempted records
    * @param shortName Provider short name
    * @param logsBasePath Root directory to write to
    * @param startTime Start time of operation
    * @param endTime End time of operation
    * @param spark SparkSession
    * @return MappingSummaryData
    */
  def buildFinalReport(results: Dataset[Row],
                       shortName: String,
                       logsBasePath: String,
                       startTime: Long,
                       endTime: Long)(implicit spark: SparkSession): MappingSummaryData = {
    import spark.implicits._

    // these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    // val oreAggregationEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    // val results: Dataset[Row] = mappingResults.map(oreAgg => oreAgg)(oreAggregationEncoder)

    val messages = MessageProcessor.getAllMessages(results)(spark)
    val warnings = MessageProcessor.getWarnings(messages)
    val errors =   MessageProcessor.getErrors(messages)

    // get counts
    val attemptedCount = results.count()
    val validRecordCount = results.filter(oreAggRow => { // FIXME This is duplicating work in MappingExecutor
      !oreAggRow // not
        .getAs[mutable.WrappedArray[Row]]("messages") // get all messages
        .map(msg => msg.getString(1)) // extract the levels into a list
        .contains(IngestLogLevel.error) // does that list contain any errors?
    }).count()

    val warnCount = warnings.count()
    val errorCount = errors.count()

    val recordErrorCount = MessageProcessor.getDistinctIdCount(errors)
    val recordWarnCount = MessageProcessor.getDistinctIdCount(warnings)

    val errorMsgDetails = MessageProcessor.getMessageFieldSummary(errors).mkString("\n")
    val warnMsgDetails = MessageProcessor.getMessageFieldSummary(warnings).mkString("\n")

    val logFileList = List(
      "all" -> messages,
      "errors" -> errors,
      "warnings" -> warnings
    ).filter { case (_, data: Dataset[_]) => data.count() > 0 }

    val logFileSeq = logFileList.map {
      case (name: String, data: Dataset[Row]) => {
        val path = logsBasePath + s"$shortName-$endTime-map-$name"
        Utils.writeLogsAsCsv(path, name, data, shortName)
        ReportFormattingUtils.centerPad(name, new File(path).getCanonicalPath)
      }
    }
    // time summary
    val timeSummary = TimeSummary(
      Utils.formatDateTime(startTime),
      Utils.formatDateTime(endTime),
      Utils.formatRuntime(endTime-startTime)
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
  /**
    * Perform the mapping for a single record
    *
    * @param document The harvested record to map
    * @param shortName Provider short name
    * @param totalCount Accumulator to track the number of records processed
    * @param successCount Accumulator to track the number of records successfully mapped
    * @param failureCount Accumulator to track the number of records that failed to map
    * @return A Row representing the mapping results (both success and failure)
    */
  def map(document: String,
          shortName: String,
          totalCount: LongAccumulator,
          successCount: LongAccumulator,
          failureCount: LongAccumulator): Row = {
    totalCount.add(1)

    val extractorClass = ProviderRegistry.lookupProfile(shortName) match {
      case Success(extClass) => extClass
      case Failure(e) => throw new RuntimeException(s"Unable to load $shortName mapping from ProviderRegistry")
    }
    val oreAggregation = extractorClass.performMapping(document)
    RowConverter.toRow(oreAggregation, model.sparkSchema)
  }
}