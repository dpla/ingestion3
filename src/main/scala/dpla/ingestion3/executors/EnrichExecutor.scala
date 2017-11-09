package dpla.ingestion3.executors

import java.io.File

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.enrichments.EnrichmentDriver
import dpla.ingestion3.model
import dpla.ingestion3.model.{ModelConverter, OreAggregation, RowConverter}
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success, Try}

trait EnrichExecutor {

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
                        i3conf: i3Conf): Unit = {

    // Verify that twofishes is reachable
    Utils.pingTwofishes(i3conf)

    logger.info("Enrichments started")

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
    val mappedRows: DataFrame = spark.read.avro(dataIn)

    // Create the enrichment outside map function so it is not recreated for each record.
    // If the Twofishes host is not reachable it will die hard
    val enrichResults: Dataset[(Row, String)] = mappedRows.map(row => {
      val driver = new EnrichmentDriver(i3conf)
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
    Utils.deleteRecursively(new File(dataOut))

    // Save mapped records out to Avro file
    successResults.toDF().write
      .format("com.databricks.spark.avro")
      .save(dataOut)

    sc.stop()

    // Clean up checkpoint directory, created above
    Utils.deleteRecursively(new File("/tmp/checkpoint"))

    // Log error messages.
    failures.foreach(msg => logger.warn(s"Enrichment error >> $msg"))

    logger.info("Enrichment finished")
    logger.info(s"Enriched ${Utils.formatNumber(successCount.value)} records")
    logger.info(s"Failed to enrich ${failureCount.value} records")
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
}
