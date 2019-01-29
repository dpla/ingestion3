package dpla.ingestion3.executors

import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.messages._
import dpla.ingestion3.model
import dpla.ingestion3.model.{OreAggregation, RowConverter}
import dpla.ingestion3.reports.summary._
import dpla.ingestion3.utils.{ProviderRegistry, Utils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions.{col, count, udf, collect_list, explode, monotonically_increasing_id}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.util.{Failure, Success}

trait MappingExecutor extends Serializable with IngestMessageTemplates {

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

    val outputPath = outputHelper.activityPath

    // @michael Any issues with making SparkSession implicit?
    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    val sc = spark.sparkContext

    val totalCount: LongAccumulator = sc.longAccumulator("Total Record Count")
    val successCount: LongAccumulator = sc.longAccumulator("Successful Record Count")

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    // these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    val oreAggregationEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    // Load the harvested record dataframe
    val harvestedRecords: DataFrame = spark.read.avro(dataIn)

    // Run the mapping over the Dataframe
    val documents: Dataset[String] = harvestedRecords.select("document").as[String]

    val dplaMap = new DplaMap()

    // All attempted records
    // Transformation only
    val mappingResults: DataFrame =
    documents.map(document =>
      dplaMap.map(document, shortName, totalCount, successCount)
    )(oreAggregationEncoder)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // User defined functions
    val dupOrigIdMsgUdf = udf((originalId: String) => duplicateOriginalId(originalId))

    val mergeMessagesUdf = udf((messages: Seq[Map[String,String]], dupOrigIdMsg: Map[String,String]) =>
      if (dupOrigIdMsg == null) messages else messages :+ dupOrigIdMsg)

    // Find records with duplicate original IDs
    val duplicateOriginalIds: DataFrame = mappingResults
      .select("originalId")
      .where("originalId != ''")
      .groupBy("originalId")
      .agg(count("*").alias("count"))
      .where("count > 1")
      .withColumn("dupOrigIdMsg", dupOrigIdMsgUdf(col("originalId")))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Find records with duplicate original IDs
//    val duplicateOriginalIds: DataFrame = mappingResults
//      .select("originalId", "id")
//      .where("originalId != ''")
//      .groupBy("originalId")
//      .agg(collect_list("id").alias("ids"))
//      .where(size(col("ids")) > 1)
//      .withColumn("dupOrigIdMsg", dupOrigIdMsgUdf(col("originalId")))
//      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val resultsWithId = mappingResults
      .join(duplicateOriginalIds, Seq("originalId"), "outer") // adds column "dupOrigIdMsg"
      .withColumn("id", monotonically_increasing_id) // adds unique identifier
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val dupOrigIdMessages = resultsWithId
      .select(col("id"), col("dupOrigIdMsg").as("msg"))

    val originalMessages = resultsWithId
      .select(col("id"), explode(col("messages")).as("msg"))

    val updatedResults = originalMessages
      .union(dupOrigIdMessages)
      .groupBy("id")
      .agg(collect_list("msg").as("messages"))
      .join(resultsWithId.drop("messages"), Seq("id"), "outer")

//    duplicateOriginalIds
//      .select(col("dupOrigIdMsg"), explode(col("ids")).as("dplaId"))


    // Add error messages to records with duplicate original IDs
//    val updatedResults: DataFrame = mappingResults
//      .join(duplicateOriginalIds, Seq("originalId"), "outer")
//      .withColumn("allMessages", mergeMessagesUdf(col("messages"), col("dupOrigIdMsg")))
//      .drop("messages")
//      .drop("dupOrigIdMsg")
//      .withColumnRenamed("allMessages", "messages")

    // Removes records from mappingResults that have at least one IngestMessage
    // with a level of IngestLogLevel.error
    // Transformation only
    val successResults: DataFrame = updatedResults
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

    // Get counts from accumulators
    val validRecordCount = successCount.count
    val attemptedCount = totalCount.count

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
      updatedResults,
      shortName,
      logsPath,
      startTime,
      endTime,
      attemptedCount,
      validRecordCount,
      duplicateOriginalIds.count)(spark)

    // Format the summary report and write it log file
    val mappingSummary = MappingSummary.getSummary(finalReport)
    outputHelper.writeSummary(mappingSummary) match {
      case Success(s) => logger.info(s"Summary written to $s.")
      case Failure(f) => logger.warn(s"Summary failed to write: $f")
    }
    // Send the mapping summary to the console as well (for convenience)
    logger.info(mappingSummary)

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
                       endTime: Long,
                       attemptedCount: Long,
                       validRecordCount: Long,
                       duplicateOriginalIds: Long)(implicit spark: SparkSession): MappingSummaryData = {
    import spark.implicits._

    // these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    // val oreAggregationEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    // Transformation
    val messages: DataFrame = MessageProcessor.getAllMessages(results)(spark)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val warnings: DataFrame = MessageProcessor.getWarnings(messages)
    val errors: DataFrame = MessageProcessor.getErrors(messages)

    // These actions evaluate `messages', `warnings', and `errors'
    val warnCount: Long = warnings.count
    val errorCount: Long = errors.count

    val logFileList: List[(String, Dataset[Row])] = List(
      "errors" -> (errors, errorCount),
      "warnings" -> (warnings, warnCount)
    ).filter { case (_, (_, count: Long)) => count > 0 } // drop empty
      .map{ case (key: String, (data: Dataset[_], _: Long)) => key -> data} // drop count

    // write out warnings and errors
    val logFileSeq: List[String] = logFileList.map {
      case (name: String, data: Dataset[Row]) =>
        val path = s"$logsBasePath/$name"
        Utils.writeLogsAsCsv(path, name, data, shortName)
        path
    }

    val recordErrorCount: Long = MessageProcessor.getDistinctIdCount(errors)
    val recordWarnCount: Long = MessageProcessor.getDistinctIdCount(warnings)

    val errorMsgDetails: String =
      MessageProcessor.getMessageFieldSummary(errors).mkString("\n")
    val warnMsgDetails: String =
      MessageProcessor.getMessageFieldSummary(warnings).mkString("\n")

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
      warnMsgDetails,
      duplicateOriginalIds
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
          successCount: LongAccumulator): Row = {
    totalCount.add(1)

    val extractorClass = ProviderRegistry.lookupProfile(shortName) match {
      case Success(extClass) => extClass
      case Failure(e) => throw new RuntimeException(s"Unable to load $shortName mapping from ProviderRegistry")
    }
    val oreAggregation: OreAggregation = extractorClass.performMapping(document)

    val hasError: Boolean =
      oreAggregation.messages.map(m => m.level).contains(IngestLogLevel.error)

    if (!hasError) successCount.add(1)

    RowConverter.toRow(oreAggregation, model.sparkSchema)
  }
}
