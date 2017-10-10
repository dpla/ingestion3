package dpla.ingestion3

import java.io.File

import com.databricks.spark.avro._
import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.enrichments.EnrichmentDriver
import dpla.ingestion3.model.{ModelConverter, OreAggregation, RowConverter}
import dpla.ingestion3.utils.Utils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.storage.StorageLevel


import scala.util.{Failure, Success, Try}

/**
  * Expects three parameters:
  *   1) a path to the harvested data
  *   2) a path to output the mapped data
  *   3) a path to the application configuration file
  *   4) provider short name
  *
  *   Usage
  *   -----
  *   To invoke via sbt:
  *     sbt "run-main dpla.ingestion3.EnrichEntry
  *     --input=/input/path/to/mapped.avro
  *     --output=/output/path/to/enriched.avro
  *     --conf=/path/to/application.conf
  *     --name=provider
  *
  */

object EnrichEntry {

  def main(args: Array[String]): Unit = {

    // Read in command line args
    val cmdArgs = new CmdArgs(args)

    val inputDir = cmdArgs.input
      .getOrElse(throw new IllegalArgumentException("Missing input dir"))
    val outputDir = cmdArgs.output
      .getOrElse(throw new IllegalArgumentException("Missing output dir"))
    val confFile = cmdArgs.configFile
      .getOrElse(throw new IllegalArgumentException("Missing conf file"))
    val shortName = cmdArgs.providerName
      .getOrElse(throw new IllegalArgumentException("Missing provider short name"))

    val enrichLogger: Logger = LogManager.getLogger("ingestion3")
    val appender = Utils.getFileAppender(shortName, "enrichment")
    enrichLogger.addAppender(appender)

    // Load configuration from file
    val i3Conf: i3Conf = new Ingestion3Conf(confFile).load()

    // Verify Twofishes can be reached
    pingTwofishes(i3Conf)

    val sparkConf = new SparkConf()
      .setAppName("Enrichment")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")
      .setMaster(i3Conf.spark.sparkMaster.getOrElse("local[*]"))

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext
    // TODO: assign checkpoint directory based on a configurable setting.
    // Consider cluster / EMR usage.
    // See https://github.com/dpla/ingestion3/pull/105
    sc.setCheckpointDir("/tmp/checkpoint")
    val totalCount: LongAccumulator = sc.longAccumulator("Total Record Count")
    val successCount: LongAccumulator = sc.longAccumulator("Successful Record Count")
    val failureCount: LongAccumulator = sc.longAccumulator("Failed Record Count")

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._
    val dplaMapDataRowEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)
    val tupleRowStringEncoder: ExpressionEncoder[(Row, String)] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    // Load the mapped records
    val mappedRows: DataFrame = spark.read.avro(inputDir)

    // Create the enrichment outside map function so it is not recreated for each record.
    // If the Twofishes host is not reachable it will die hard
    val enrichResults: Dataset[(Row, String)] = mappedRows.map(row => {
      val driver = new EnrichmentDriver(i3Conf)
      Try{ ModelConverter.toModel(row) } match {
        case Success(dplaMapData) =>
          enrich(dplaMapData, driver, totalCount, successCount, failureCount)
        case Failure(err) =>
          (null, s"Error parsing mapped data: ${err.getMessage}\n" +
                 s"${err.getStackTrace.mkString("\n")}")
      }
    })(tupleRowStringEncoder)
       .persist(StorageLevel.DISK_ONLY)
       .checkpoint()

    val successResults: Dataset[Row] = enrichResults
      .filter(tuple => Option(tuple._1).isDefined)
      .map(tuple => tuple._1)(dplaMapDataRowEncoder)

    val failures:  Array[String] = enrichResults
      .filter(tuple => Option(tuple._2).isDefined)
      .map(tuple => tuple._2).collect()

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(outputDir))

    // Save mapped records out to Avro file
    successResults.toDF().write
      .format("com.databricks.spark.avro")
      .save(outputDir)

    sc.stop()

    // Clean up checkpoint directory, created above
    Utils.deleteRecursively(new File("/tmp/checkpoint"))

    // Log error messages.
    failures.foreach(msg => enrichLogger.warn(s"Error: ${msg}"))

    val statusMsg =
      s"Mapped ${totalCount.value} records and enriched " +
      s"${successCount.value} records.\n" +
      s"${failureCount.value} enrichment errors"
    enrichLogger.debug(statusMsg)
    println(statusMsg)

  }

  private def enrich(dplaMapData: OreAggregation,
                     driver: EnrichmentDriver,
                     totalCount: LongAccumulator,
                     successCount: LongAccumulator,
                     failureCount: LongAccumulator): (Row, String) = {
    totalCount.add(1)
    driver.enrich(dplaMapData) match {
      case Success(enriched) =>
        successCount.add(1)
        (RowConverter.toRow(enriched, model.sparkSchema), null)
      case Failure(exception) =>
        failureCount.add(1)
        (null, s"${exception.getMessage}\n" +
          s"${exception.getStackTrace.mkString("\n")}")

    }
  }

  /**
    * Attempts to reach the Twofishes service
    *
    * @param conf Configuration file
    * @throws RuntimeException If the service cannot be reached
    */
  private def pingTwofishes(conf: i3Conf): Unit = {
    val host = conf.twofishes.hostname.getOrElse("localhost")
    val port = conf.twofishes.port.getOrElse("8081")
    Utils.validateUrl(s"http://${host}:${port}/query") match {
      case true => Unit // TODO log something?
      case false => throw new RuntimeException(s"Cannot reach Twofishes at ${host}")
    }
  }
}
