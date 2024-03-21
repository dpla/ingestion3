package dpla.ingestion3.entries.reports

import dpla.ingestion3.dataStorage.OutputHelper

import java.time.LocalDateTime
import dpla.ingestion3.utils.Utils
import dpla.ingestion3.model.{ModelConverter, OreAggregation}
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success}

object ReporterMain {

  private val fieldedRptList = Seq(
    "propertyDistinctValue",
    "propertyValue"
  )

  private val reportFields = Seq(
    "dataProvider",
    "isShownAt",
    "edmRights",
    "intermediateProvider",
    "preview",
    "sourceResource.collection.title",
    "sourceResource.contributor.name",
    "sourceResource.creator.name",
    "sourceResource.date.originalSourceDate",
    "sourceResource.description",
    "sourceResource.extent",
    "sourceResource.format",
    "sourceResource.genre",
    "sourceResource.identifier",
    "sourceResource.language.providedLabel",
    "sourceResource.language.concept",
    "sourceResource.place.name",
    "sourceResource.publisher.name",
    "sourceResource.relation",
    "sourceResource.replacedBy",
    "sourceResource.replaces",
    "sourceResource.rights",
    "sourceResource.rightsHolder.name",
    "sourceResource.subject.providedLabel",
    "sourceResource.temporal.originalSourceDate",
    "sourceResource.title",
    "sourceResource.type",
    "sourceResource.alternateTitle"
  )

  private val thumbnailOpts = Seq(
    "missing",
    "preview"
    // "dimensions" // Left out for performance considerations
  )

  def usage(): Unit = {
    println("""
        |Usage:
        |
        |ReporterMain <input> <output> <spark master> <report token> \
        |            [<param> ...]
      """.stripMargin)
  }
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      usage()
      System.err.println("Incorrect invocation arguments")
      sys.exit(1)
    }
    val inputURI = args(0)
    val outputURI = args(1)
    val sparkMaster = args(2)
    val reportName = args(3)
    val reportParams = args.slice(4, args.length)

    val sparkConf = new SparkConf().setMaster(sparkMaster)
    val logger = Utils.createLogger("reports")

    // Start spark session
    implicit val spark = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", false)
      .getOrCreate()

    val sc = spark.sparkContext

    // Read data in
    val inputDF: DataFrame = spark.read.format("avro").load(inputURI)

    val mappedData: Dataset[OreAggregation] = dplaMapData(inputDF)

    executeReport(
      spark,
      mappedData,
      outputURI,
      reportName,
      reportParams,
      logger
    )

    sc.stop
  }

  /** Single entry point for executing all reports
    *
    * @param sparkConf
    *   Spark configuration
    * @param input
    *   Path to data set to report against
    * @param baseOutput
    *   Base path for where to save all reports.
    */
  def executeAllReports(
      sparkConf: SparkConf,
      input: String,
      baseOutput: String,
      shortName: String,
      logger: Logger
  ): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(baseOutput, shortName, "reports", startDateTime)

    val reportsPath = outputHelper.activityPath

    // Start spark session
    implicit val spark = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", false)
      .getOrCreate()

    val sc = spark.sparkContext

    // Read data in
    val inputDF: DataFrame = spark.read.format("avro").load(input)

    val mappedData: Dataset[OreAggregation] =
      dplaMapData(inputDF).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Metadata completion report
    logger.info(s"Executing metadataCompleteness report")
    executeReport(
      spark,
      mappedData,
      s"$reportsPath/metadataCompleteness",
      "metadataCompleteness",
      logger = logger
    )

    // Thumbnail reports
    thumbnailOpts.foreach(rptOpt => {
      logger.info(s"Executing thumbnail report for $rptOpt")
      executeReport(
        spark,
        mappedData,
        s"$reportsPath/thumbnail/$rptOpt",
        "thumbnail",
        Array(rptOpt),
        logger
      )
    })

    // Property distinct value
    reportFields.foreach(field => {
      val rptOut = s"$reportsPath/propertyDistinctValue/$field"
      logger.info(s"Executing propertyDistinctValue for $field")
      executeReport(
        spark,
        mappedData,
        rptOut,
        "propertyDistinctValue",
        Array(field),
        logger
      )
    })

    // Property value
    reportFields.foreach(field => {
      val rptOut = s"$reportsPath/propertyValue/$field"
      logger.info(s"Executing propertyValue for $field")
      executeReport(
        spark,
        mappedData,
        rptOut,
        "propertyValue",
        Array(field),
        logger
      )
    })

    // Enrichment meta information
    // TODO we should export the version of the language map used to enrich the data so there is a closer 1:1
    // relationship that data folks can follow-up on.

    // Write manifest
    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Reports",
      "Provider" -> shortName,
      "Input" -> input
    )
    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    // Stop spark
    sc.stop

    // Return reports path.
    reportsPath
  }

  def dplaMapData(input: DataFrame): Dataset[OreAggregation] = {
    implicit val dplaMapDataEncoder: Encoder[OreAggregation] =
      org.apache.spark.sql.Encoders.kryo[OreAggregation]

    input.map(row => ModelConverter.toModel(row))
  }

  /** Execute the report
    *
    * @param sparkConf
    *   Spark configurations
    * @param input
    *   Source data to report on
    * @param output
    *   Destination for reports
    * @param reportName
    *   Name of report to run
    * @param reportParams
    *   Supplemental report params
    * @return
    */
  def executeReport(
      spark: SparkSession,
      input: Dataset[OreAggregation],
      output: String,
      reportName: String,
      reportParams: Array[String] = Array(),
      logger: Logger
  ): Unit = {

    new Reporter(spark, reportName, input, reportParams, logger).main() match {
      case Success(rpt) =>
        rpt
          .repartition(1)
          .write
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .save(output)

      case Failure(ex) => logger.error(ex.toString)
    }
  }
}
