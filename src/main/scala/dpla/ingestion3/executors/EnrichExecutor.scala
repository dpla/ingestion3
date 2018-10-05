package dpla.ingestion3.executors

import java.io.File
import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.enrichments.EnrichmentDriver
import dpla.ingestion3.messages._
import dpla.ingestion3.model
import dpla.ingestion3.model.{ModelConverter, OreAggregation, RowConverter}
import dpla.ingestion3.reports.PrepareEnrichmentReport
import dpla.ingestion3.reports.summary._
import dpla.ingestion3.utils.{OutputHelper, Utils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

trait EnrichExecutor extends Serializable {

  /**
    * Execute the enrichments
    * @param sparkConf Spark configuration
    * @param dataIn Mapped data
    * @param dataOut Location to save output
    * @param shortName Provider short name
    * @param logger Logger
    */
  def executeEnrichment(sparkConf: SparkConf,
                        dataIn: String,
                        dataOut: String,
                        shortName: String,
                        logger: Logger,
                        i3conf: i3Conf): String = {

    // Verify that twofishes is reachable
    // Utils.pingTwofishes(i3conf)

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    // This start time is used to measure the duration of enrichment.
    val startTime = System.currentTimeMillis()

    val outputHelper =
      new OutputHelper(dataOut, shortName, "enrichment", startDateTime)

    val outputPath = outputHelper.outputPath

    implicit val spark = SparkSession.builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", false)
      .getOrCreate()

    val sc = spark.sparkContext
    val improvedCount: LongAccumulator = sc.longAccumulator("Improved Record Count")

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._
    val dplaMapDataRowEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)
    val tupleRowStringEncoder: ExpressionEncoder[(Row, String)] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    // Load the mapped records
    val mappedRows: DataFrame = spark.read.avro(dataIn)

    // Create the enrichment outside map function so it is not recreated for each record.
    // If the Twofishes host is not reachable it will die hard
    val enrichResults: Dataset[(Row, String)] = mappedRows.map(row => {
      val driver = new EnrichmentDriver(i3conf)
      Try{ ModelConverter.toModel(row) } match {
        case Success(dplaMapData) =>
          enrich(dplaMapData, driver, improvedCount)
        case Failure(err) =>
          (null, s"Error parsing mapped data: ${err.getMessage}\n" +
            s"${err.getStackTrace.mkString("\n")}")
      }
    })(tupleRowStringEncoder)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val successResults: Dataset[Row] = enrichResults
      .filter(tuple => Option(tuple._1).isDefined)
      .map(tuple => tuple._1)(dplaMapDataRowEncoder)

    val failures:  Array[String] = enrichResults
      .filter(tuple => Option(tuple._2).isDefined)
      .map(tuple => tuple._2).collect()

    // Get all the messages for all records
    val messages = MessageProcessor.getAllMessages(successResults)(spark)

    // Save mapped records out to Avro file
    successResults.toDF().write
      .format("com.databricks.spark.avro")
      .save(outputPath)

    // Create and write manifest.

    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Enrichment",
      "Provider" -> shortName,
      "Record count" -> successResults.count.toString,
      "Input" -> dataIn
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    // Write logs and summary reports.

    val endTime = System.currentTimeMillis()

    val typeMessages = messages.filter("field='type'")
    val dateMessages = messages.filter("field='date'")
    val langMessages = messages.filter("field='language'")
    val placeMessages = messages.filter("field='place'")

    val logEnrichedFields = List(
      "all" -> messages,
      "type" -> typeMessages,
      "date" -> dateMessages,
      "language" -> langMessages,
      "place" -> placeMessages
    )
      .filter { case (_, data: Dataset[_]) => data.count() > 0 }

    val logFileSeq = logEnrichedFields.map {
      case (name: String, data: Dataset[_]) => {
        val path = outputHelper.logsBasePath + s"$shortName-$endTime-enrich-$name"
        data match {
          case dr: Dataset[Row] => Utils.writeLogsAsCsv(path, name, dr, shortName)
        }
        val canonicalPath = if (path.startsWith("s3a://")) path else
          new File(path).getCanonicalPath
        ReportFormattingUtils.centerPad(name, canonicalPath)
      }
    }

    // build the information for reporting
    val attemptedCount = mappedRows.count()

    val timeSummary = TimeSummary(
      Utils.formatDateTime(startTime),
      Utils.formatDateTime(endTime),
      Utils.formatRuntime(endTime-startTime)
    )

    val operationSummary = OperationSummary(
      attemptedCount,
      improvedCount.count,
      attemptedCount-improvedCount.count,
      logFileSeq
    )

    val enrichOpSummary = EnrichmentOpsSummary(
      typeMessages.count,
      dateMessages.count,
      langMessages.count,
      placeMessages.count,
      langSummary = PrepareEnrichmentReport.generateFieldReport(messages, "language"),
      typeSummary = PrepareEnrichmentReport.generateFieldReport(messages, "type"),
      placeSummary = PrepareEnrichmentReport.generateFieldReport(messages, "place")
    )

    val summaryData = EnrichmentSummaryData(shortName, operationSummary, timeSummary, enrichOpSummary)

    // Write warn and error messages to CSV files
    logger.info(EnrichmentSummary.getSummary(summaryData))

    sc.stop()

    // Log error messages.
    failures.foreach(logger.error(_))

    // Return output destination of enriched records.
    outputPath
  }

  private def enrich(dplaMapData: OreAggregation,
                     driver: EnrichmentDriver,
                     improvedCount: LongAccumulator): (Row, String) = {
    driver.enrich(dplaMapData) match {
      case Success(enriched) =>
        implicit val msgs = new MessageCollector[IngestMessage]()

        val oreAggMitMsgs = PrepareEnrichmentReport.prepareEnrichedData(enriched, dplaMapData)(msgs)

        if(!dplaMapData.sourceResource.equals(oreAggMitMsgs.sourceResource))
          improvedCount.add(1) // captures normalizations and enrichments

        (RowConverter.toRow(oreAggMitMsgs, model.sparkSchema), null)
      case Failure(exception) =>
        (null, s"${exception.getMessage}")
    }
  }
}
