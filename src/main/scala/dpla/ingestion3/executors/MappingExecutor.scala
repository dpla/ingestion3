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
                      logger: Logger): Unit = {

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

    val tupleRowStringEncoder: ExpressionEncoder[(Row, String)] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    // Load the harvested record dataframe
    val harvestedRecords: DataFrame = spark.read.avro(dataIn).repartition(1024)

    // Run the mapping over the Dataframe
    val documents: Dataset[String] = harvestedRecords.select("document").as[String]

    val dplaMap = new DplaMap()

    val mappingResults: Dataset[(Row, String)] =
      documents.map(document =>
        dplaMap.map(document, shortName,
          totalCount, successCount, failureCount)
      )(tupleRowStringEncoder)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val successResults: Dataset[Row] = mappingResults
      .filter(tuple => Option(tuple._1).isDefined)
      .map(tuple => tuple._1)(oreAggregationEncoder)

    // Results must be written before _LOGS.
    // Otherwise, spark interpret the `successResults' `outputPath' as
    // already existing, and will fail to write.
    successResults.where("size(messages.level) == 0").toDF().write.avro(outputPath)

//    val endTime = System.currentTimeMillis()

//    val logsBasePath = outputHelper.logsBasePath

//    // Collect the values needed to generate the report
//    val finalReport = buildFinalReport(successResults, mappingResults, shortName, logsBasePath, startTime, endTime)(spark)
//    // Format the summary report and write it log file
//    logger.info(MappingSummary.getSummary(finalReport))

    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Mapping",
      "Provider" -> shortName,
      "Record count" -> successResults.count.toString,
      "Input" -> dataIn
    )
    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    spark.stop()
  }

  /**
    * Creates a summary report of the ingest by using the MappingSummary object
    *
    * @param successResults
    * @param mappingResults
    * @param shortName
    * @param spark
    * @return
    */
  def buildFinalReport(successResults: Dataset[Row],
                       mappingResults: Dataset[(Row, String)],
                       shortName: String,
                       logsBasePath: String,
                       startTime: Long,
                       endTime: Long)(implicit spark: SparkSession): MappingSummaryData = {
    import spark.implicits._

    // these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    val oreAggregationEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    val sc = spark.sparkContext

    val results: Dataset[Row] = mappingResults
      .filter(tuple => Option(tuple._1).isDefined)
      .map(tuple => tuple._1)(oreAggregationEncoder)

    val exceptions: Array[String] = mappingResults
      .filter(tuple => Option(tuple._2).isDefined)
      .map(tuple => tuple._2).collect()

    val messages = MessageProcessor.getAllMessages(results)(spark)
    val warnings = MessageProcessor.getWarnings(messages)
    val errors =   MessageProcessor.getErrors(messages)

    // get counts
    val attemptedCount = mappingResults.count() // successResults.count()
    val validCount = results.select("dplaUri").where("size(messages) == 0").count()
    val warnCount = warnings.count()
    val errorCount = errors.distinct().count()

    val recordErrorCount = MessageProcessor.getDistinctIdCount(errors)
    val recordWarnCount = MessageProcessor.getDistinctIdCount(warnings)

    val errorMsgDetails = MessageProcessor.getMessageFieldSummary(errors).mkString("\n")
    val warnMsgDetails = MessageProcessor.getMessageFieldSummary(warnings).mkString("\n")

    val exceptionsDS = sc.parallelize(exceptions).toDS()

    val logFileList = List(
      "all" -> messages,
      "errors" -> errors,
      "warnings" -> warnings,
      "exceptions" -> exceptionsDS
    )//.filter { case (_, data: Dataset[_]) => data.count() > 0 }

    val logFileSeq = logFileList.map {
      case (name: String, data: Dataset[_]) => {
        val path = s"$logsBasePath$name"
        data match {
          case dr: Dataset[Row] => Utils.writeLogsAsCsv(path, name, dr, shortName)
          case ds: Dataset[String] => Utils.writeLogsAsTxt(path, name, ds, shortName)
        }
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
      validCount,
      recordErrorCount,
      logFileSeq
    )
    // messages summary
    val messageSummary = MessageSummary(
      errorCount,
      exceptions.length,
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
    * @return A tuple (Row, String)
    *           - (Row, null) on successful mapping
    *           - (null, Error message) on mapping failure
    */
  def map(document: String,
          shortName: String,
          totalCount: LongAccumulator,
          successCount: LongAccumulator,
          failureCount: LongAccumulator): (Row, String) = {
    totalCount.add(1)

    val extractorClass = ProviderRegistry.lookupProfile(shortName) match {
      case Success(extClass) => extClass
      case Failure(e) => throw new RuntimeException(s"Unable to load $shortName mapping from ProviderRegistry")
    }
    extractorClass.performMapping(document) match {
      case (Some(oreAgg), Some(exception)) => (RowConverter.toRow(oreAgg, model.sparkSchema), exception)
      case (Some(oreAgg), None) => (RowConverter.toRow(oreAgg, model.sparkSchema), null)
      case (None, Some(exception)) => (null, exception)
      case _ => (null, null)
    }
  }
}