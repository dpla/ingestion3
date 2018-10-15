package dpla.ingestion3.entries.reports

import java.time.LocalDateTime

import dpla.ingestion3.utils.Utils
import dpla.ingestion3.dataStorage.OutputHelper
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

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
    println(
      """
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
    val reportName = args(3)
    val reportParams = args.slice(4, args.length)

    val sparkConf = new SparkConf().setMaster("local[1]")
    val logger = Utils.createLogger("reports")

    // This is a pretty bogus default. Should read from config file...
    executeReport(sparkConf, inputURI, outputURI, reportName, reportParams, logger)
  }


  /**
    * Single entry point for executing all reports
    *
    * @param sparkConf Spark configuration
    * @param input Path to data set to report against
    * @param baseOutput Base path for where to save all reports.
    */
  def executeAllReports(sparkConf: SparkConf,
                        input: String,
                        baseOutput: String,
                        shortName: String,
                        logger: Logger): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(baseOutput, shortName, "reports", startDateTime)

    val reportsPath = outputHelper.activityPath

    // Property value / Property distinct value
    fieldedRptList.map(rpt =>
      reportFields.map(field => {
        val rptOut = s"$reportsPath/$rpt/$field"
        logger.info(s"Executing $rpt for $field")
        executeReport(sparkConf, input, rptOut, rpt, Array(field), logger)
      }
    ))

    // Metadata completion report
    logger.info(s"Executing metadataCompleteness report")
    executeReport(sparkConf, input, s"$reportsPath/metadataCompleteness", "metadataCompleteness", logger = logger)

    // thumbnail report options
    thumbnailOpts.foreach(rptOpt => {
      logger.info(s"Executing thumbnail report for $rptOpt")
      executeReport(sparkConf, input, s"$reportsPath/thumbnail/$rptOpt", "thumbnail", Array(rptOpt), logger)
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

    // Return reports path.
    reportsPath
  }

  /**
    * Execute the report
    *
    * @param sparkConf Spark configurations
    * @param input Source data to report on
    * @param output Destination for reports
    * @param reportName Name of report to run
    * @param reportParams Supplemental report params
    * @return
    */
  def executeReport(sparkConf: SparkConf,
                    input: String,
                    output: String,
                    reportName: String,
                    reportParams: Array[String] = Array(),
                    logger: Logger): Unit = {

    new Reporter(sparkConf, reportName, input, output, reportParams, logger)
      .main()
  }
}
