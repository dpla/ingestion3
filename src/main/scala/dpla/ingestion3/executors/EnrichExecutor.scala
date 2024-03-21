package dpla.ingestion3.executors

import java.io.File
import java.time.LocalDateTime
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.enrichments.EnrichmentDriver
import dpla.ingestion3.messages._
import dpla.ingestion3.model
import dpla.ingestion3.model.{ModelConverter, OreAggregation, RowConverter}
import dpla.ingestion3.reports.PrepareEnrichmentReport
import dpla.ingestion3.reports.summary._
import dpla.ingestion3.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  Encoder,
  Encoders,
  Row,
  SparkSession
}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

trait EnrichExecutor extends Serializable {

  /** Execute the enrichments
    * @param sparkConf
    *   Spark configuration
    * @param dataIn
    *   Mapped data
    * @param dataOut
    *   Location to save output
    * @param shortName
    *   Provider short name
    * @param logger
    *   Logger
    */
  def executeEnrichment(
      sparkConf: SparkConf,
      dataIn: String,
      dataOut: String,
      shortName: String,
      logger: Logger,
      i3conf: i3Conf
  ): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    // This start time is used to measure the duration of enrichment.
    val startTime = System.currentTimeMillis()

    val outputHelper =
      new OutputHelper(dataOut, shortName, "enrichment", startDateTime)

    val outputPath = outputHelper.activityPath

    implicit val spark = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", false)
      .getOrCreate()

    val sc = spark.sparkContext

    val attemptedCount: LongAccumulator =
      sc.longAccumulator("Total Mapped Record Count")
    val successCount: LongAccumulator =
      sc.longAccumulator("Total Enriched Record Count")
    val improvedCount: LongAccumulator =
      sc.longAccumulator("Improved Record Count")

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._
    val dplaMapDataRowEncoder: Encoder[Row] =
      RowEncoder.encoderFor(model.sparkSchema)
    val tupleRowStringEncoder: Encoder[(Row, String)] = ExpressionEncoder()

    // Load the mapped records
    val mappedRows: DataFrame = spark.read.format("avro").load(dataIn)

    // Wrapping an instance of EnrichmentDriver in an object allows it to be
    // used in distributed operations. Without the object wrapper, it throws
    // NotSerializableException errors, which originate in several of the
    // individual enrichments.
    object SharedDriver {
      val driver = new EnrichmentDriver(i3conf)
      def get: EnrichmentDriver = driver
    }

    // Create the enrichment outside map function so it is not recreated for each record.
    // Transformation
    val enrichResults = mappedRows.map(row => {
      val driver = SharedDriver.get
      attemptedCount.add(1)
      Try { ModelConverter.toModel(row) } match {
        case Success(dplaMapData) =>
          enrich(dplaMapData, driver, improvedCount, successCount)
        case Failure(err) =>
          (
            null,
            s"Error parsing mapped data: ${err.getMessage}\n" +
              s"${err.getStackTrace.mkString("\n")}"
          )
      }
    })(tupleRowStringEncoder)

    // Transformation
    val successResults: Dataset[Row] = enrichResults
      .filter(tuple => Option(tuple._1).isDefined)
      .map(tuple => tuple._1)(dplaMapDataRowEncoder)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Save mapped records out to Avro file
    // Action
    successResults
      .toDF()
      .write
      .format("com.databricks.spark.avro")
      .save(outputPath)

    // Create and write manifest.
    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Enrichment",
      "Provider" -> shortName,
      "Record count" -> Utils.formatNumber(successCount.count),
      "Input" -> dataIn
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    // Write logs and summary reports.
    val endTime = System.currentTimeMillis()

    // Get all the messages for all records.
    val messages: DataFrame = MessageProcessor
      .getAllMessages(successResults)(spark)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Transformations.
    val typeMessages = messages.filter("field='type'")
    val dateMessages = messages.filter("field='date'")
    val langMessages = messages.filter("field='language'")
    val placeMessages = messages.filter("field='place'")
    val dataProviderMessages =
      messages.filter("field='dataProvider.exactMatch.URI'")
    val providerMessages = messages.filter("field='provider.exactMatch.URI'")

    // Compute the counts of different message types.
    // Actions
    val typeMessagesCount: Long = typeMessages.count
    val dateMessagesCount: Long = dateMessages.count
    val langMessagesCount: Long = langMessages.count
    val placeMessagesCount: Long = placeMessages.count
    val dataProviderMessagesCount: Long = dataProviderMessages.count
    val providerMessagesCount: Long = providerMessages.count

    val logEnrichedFields: List[(String, Dataset[Row])] = List(
      "type" -> (typeMessages, typeMessagesCount),
      "date" -> (dateMessages, dateMessagesCount),
      "language" -> (langMessages, langMessagesCount),
      "place" -> (placeMessages, placeMessagesCount),
      "dataProvider" -> (dataProviderMessages, dataProviderMessagesCount),
      "provider" -> (providerMessages, providerMessagesCount)
    ).filter { case (_, (_, count: Long)) => count > 0 } // drop empty
      .map { case (key: String, (data: Dataset[_], _: Long)) =>
        key -> data
      } // drop count

    // This action re-evaluates `typeMessages', `dateMessages', etc.
    val logFileSeq = logEnrichedFields.map {
      case (name: String, data: Dataset[_]) => {
        val path = outputHelper.logsPath + s"/$name"
        Utils.writeLogsAsCsv(path, name, data, shortName)
        outputHelper.s3Address match {
          case Some(_) => path
          case None    => new File(path).getCanonicalPath
        }
      }
    }

    val timeSummary = TimeSummary(
      Utils.formatDateTime(startTime),
      Utils.formatDateTime(endTime),
      Utils.formatRuntime(endTime - startTime)
    )

    val operationSummary = OperationSummary(
      attemptedCount.count,
      improvedCount.count,
      attemptedCount.count - improvedCount.count,
      logFileSeq
    )

    // `generateFieldReport' is a shuffle operation and an action
    val enrichOpSummary = EnrichmentOpsSummary(
      typeImproved = typeMessagesCount,
      dateImproved = dateMessagesCount,
      langImproved = langMessagesCount,
      placeImprove = placeMessagesCount,
      dataProviderImprove = dataProviderMessagesCount,
      providerImprove = providerMessagesCount,
      langSummary =
        PrepareEnrichmentReport.generateFieldReport(messages, "language"),
      typeSummary =
        PrepareEnrichmentReport.generateFieldReport(messages, "type"),
      placeSummary =
        PrepareEnrichmentReport.generateFieldReport(messages, "place"),
      dateSummary =
        PrepareEnrichmentReport.generateFieldReport(messages, "date"),
      dataProviderSummary = PrepareEnrichmentReport
        .generateFieldReport(messages, "dataProvider.exactMatch.URI"),
      providerSummary = PrepareEnrichmentReport.generateFieldReport(
        messages,
        "provider.exactMatch.URI"
      )
    )

    // Calculate data points for enrichment summary
    val summaryData = EnrichmentSummaryData(
      shortName,
      operationSummary,
      timeSummary,
      enrichOpSummary
    )
    // Builds text blob
    val enrichSummary = EnrichmentSummary.getSummary(summaryData)

    // Write enrich summary to _SUMMARY
    outputHelper.writeSummary(enrichSummary) match {
      case Success(s) => logger.info(s"Summary written to $s.")
      case Failure(f) => logger.warn(s"Summary failed to write: $f")
    }
    // For convenience
    logger.info(enrichSummary)

    sc.stop()

    // Return output destination of enriched records.
    outputPath
  }

  private def enrich(
      dplaMapData: OreAggregation,
      driver: EnrichmentDriver,
      improvedCount: LongAccumulator,
      successCount: LongAccumulator
  ): (Row, String) = {
    driver.enrich(dplaMapData) match {
      case Success(enriched) =>
        implicit val msgs = new MessageCollector[IngestMessage]()

        val oreAggMitMsgs =
          PrepareEnrichmentReport.prepareEnrichedData(enriched, dplaMapData)(
            msgs
          )

        // count all records that were normalized or enriched
        if (!dplaMapData.sourceResource.equals(oreAggMitMsgs.sourceResource))
          improvedCount.add(1)

        // count all records that did not have terminal errors
        successCount.add(1)

        (RowConverter.toRow(oreAggMitMsgs, model.sparkSchema), null)
      case Failure(exception) =>
        (null, s"${exception.getMessage}")
    }
  }
}
