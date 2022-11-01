package dpla.ingestion3.executors

import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.eleanor.Schemata.Ebook
import dpla.eleanor.profiles.EbookProfile
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.messages._
import dpla.ingestion3.model
import dpla.ingestion3.model.{ModelConverter, OreAggregation, RowConverter}
import dpla.ingestion3.profiles.CHProfile
import dpla.ingestion3.reports.summary._
import dpla.ingestion3.utils.{CHProviderRegistry, Utils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.storage.StorageLevel
import org.json4s.JsonAST.JValue

import scala.collection.mutable
import scala.util.{Failure, Success}
import scala.xml.NodeSeq

trait MappingExecutor extends Serializable with IngestMessageTemplates {

  /**
    * Lookup the profile and mapping class
    * TODO this could be better (accepts registry as param etc.)
   */
  def getExtractorClass(shortName: String): CHProfile[_ >: NodeSeq with JValue] =
    CHProviderRegistry.lookupProfile(shortName) match {
      case Success(extClass) => extClass
      case Failure(e) => throw new RuntimeException(s"Unable to load $shortName mapping from CHProviderRegistry")
    }

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

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    // these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    val oreAggregationEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    val dplaMap = new DplaMap()

    val harvestedRecords: DataFrame = spark.read.avro(dataIn)

    // Get distinct harvest record count
    val distinctHarvestCount: Long = harvestedRecords.distinct.count

    // Run the mapping over the Dataframe
    // Transformation only
    val extractorClass = getExtractorClass(shortName) // lookup from registry

    // Repartition based on number of available threads
    // This assumes running in standalone mode (one executor)
    val coresPerExecutor = spark.sparkContext.range(0,1)
      .map(_ => java.lang.Runtime.getRuntime.availableProcessors).collect.head

    val mappingResults: RDD[OreAggregation] = harvestedRecords
      .repartition(coresPerExecutor)
      .select("document")
      .as[String]
      .rdd
      .map(document => dplaMap.map(document, extractorClass))

    // Encode to Row-based structure
    // Must be encoded to save to parquet
    val encodedMappingResults: DataFrame =
      spark.createDataset(
        mappingResults.map(oreAgg => RowConverter.toRow(oreAgg, model.sparkSchema))
      )(oreAggregationEncoder)

    // Save mapped results locally as parquet
    val tempLocation1 = s"/tmp/$shortName-intermediate-results-1.parquet"
    encodedMappingResults.write.parquet(tempLocation1)
    val intermediateResults1: DataFrame = spark.read.parquet(tempLocation1)

    // For reporting purposes, calculate number of duplicate harvest records
    // Since nothing has been filtered yet, the number of mapped intermediate results equals
    // the number of harvested records
    val totalCount: Long = intermediateResults1.count
    val duplicateHarvest: Long = totalCount - distinctHarvestCount

    // TODO only do this if enforceDuplicateIds is true
    // Get a list of originalIds that appear in more than one record
    val duplicateOriginalIds: Broadcast[Array[String]] =
      spark.sparkContext.broadcast(
        intermediateResults1
          .select("originalId")
          .rdd
          .map(_.getString(0))
          .countByValue  // action, forces evaluation
          .collect{ case(origId, count) if count > 1 && origId != "" => origId }
          .toArray
      )

    // Update messages to include duplicate originalId
    // Since we are changing the contents of the messages column,
    // we convert back to rows of OreAggregation to make the change,
    // and then re-encode to ensure that the final encoding is correct.
    val enforceDuplidateIds: Boolean = getExtractorClass(shortName).getMapping.enforceDuplicateIds

    val updatedResults: RDD[OreAggregation] = intermediateResults1
      .map(row => ModelConverter.toModel(row))
      .map(oreAgg => {
        oreAgg.copy(messages =
          if (duplicateOriginalIds.value.contains(oreAgg.originalId))
            duplicateOriginalId(oreAgg.originalId, enforceDuplidateIds) +: oreAgg.messages // prepend is faster that append on seq
          else
            oreAgg.messages
        )
      })
      .rdd

    // Encode to Row-based structure
    val encodedUpdatedResults: DataFrame =
      spark.createDataset(
        updatedResults.map(oreAgg => RowConverter.toRow(oreAgg, model.sparkSchema))
      )(oreAggregationEncoder)

    // Save mapped results locally as parquet
    val tempLocation2 = s"/tmp/$shortName-intermediate-results-2.parquet"
    encodedUpdatedResults.write.parquet(tempLocation2)
    val intermediateResults2: DataFrame = spark.read.parquet(tempLocation2)

    // Must evaluate encodedMappingResults before successResults is called.
    // Otherwise spark will attempt to evaluate the filter transformation before the encoding transformation.
//    val totalCount = encodedMappingResults.count

    // Removes records from mappingResults that have at least one IngestMessage
    // with a level of IngestLogLevel.error
    // Transformation only
    val successResults: DataFrame = intermediateResults2
      .filter(oreAggRow => {
        !oreAggRow // not
          .getAs[mutable.WrappedArray[Row]]("messages") // get all messages
          .map(msg => msg.getString(1)) // extract the levels into a list
          .contains(IngestLogLevel.error) // does that list contain any errors?
      })

    // Results must be written before _LOGS.
    // Otherwise, spark interpret the `successResults' `outputPath' as
    // already existing, and will fail to write.
    successResults.write.avro(outputPath)

    // Get counts
    val validRecordCount = spark.read.avro(outputPath).count // requires read-after-write consistency
    val attemptedCount = totalCount

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
      intermediateResults2,
      shortName,
      logsPath,
      startTime,
      endTime,
      attemptedCount,
      validRecordCount,
      duplicateHarvest)(spark)

    // Format the summary report and write it log file
    val mappingSummary = MappingSummary.getSummary(finalReport)
    outputHelper.writeSummary(mappingSummary) match {
      case Success(s) => logger.info(s"Summary written to $s.")
      case Failure(f) => logger.warn(s"Summary failed to write: $f")
    }
    // Send the mapping summary to the console as well (for convenience)
    logger.info(mappingSummary)

    spark.stop()

    // TODO delete tmp files

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
                       duplicateHarvestRecords: Long)(implicit spark: SparkSession): MappingSummaryData = {
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
      Utils.formatRuntime(endTime-startTime)
    )
    // operation summary
    val operationSummary = OperationSummary(
      attemptedCount,
      validRecordCount,
      recordErrorCount,
      logFileSeq,
      duplicateHarvestRecords
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
    * @param extractorClass Mapping/extraction class defined in CHProfile
    *
    * @return An OreAggregation representing the mapping results (both success and failure)
    */
  def map(document: String, extractorClass: CHProfile[_ >: NodeSeq with JValue]): OreAggregation = {
    extractorClass.mapOreAggregation(document)
  }
}

// TODO Merge DplaMap and EbookMap --- duplicative for now
/**
  *
  */
class EbookMap extends Serializable {
  def map(document: String, extractorClass: EbookProfile[_ >: NodeSeq with JValue]): Ebook = {
    val oreAggregation = extractorClass.mapOreAggregation(document) // maps the OreAggregation piece of an ebook
    val payloads = extractorClass.mapPayload(document) // maps the Payload piece of an ebook
    Ebook(oreAggregation, payloads)
  }
}
