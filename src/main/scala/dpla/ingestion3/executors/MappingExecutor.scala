package dpla.ingestion3.executors

import java.io.File

import com.databricks.spark.avro._
import dpla.ingestion3.mappers.providers.Extractor
import dpla.ingestion3.model
import dpla.ingestion3.model.RowConverter
import dpla.ingestion3.utils.{ProviderRegistry, Utils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.util.{Failure, Success}

trait MappingExecutor {

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

    logger.info("Mapping started")
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

    // these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    val oreAggregationEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    val tupleRowStringEncoder: ExpressionEncoder[(Row, String)] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    // Load the harvested record dataframe
    val harvestedRecords: DataFrame = spark.read.avro(dataIn).repartition(1024)

    // Look up a registered Extractor class with the given shortName.
    val extractorClass = ProviderRegistry.lookupExtractorClass(shortName) match {
      case Success(extClass) => extClass
      case Failure(e) =>
        logger.fatal(e.getMessage)
        throw e
    }

    // Run the mapping over the Dataframe
    val documents: Dataset[String] = harvestedRecords.select("document").as[String]

    val mappingResults: Dataset[(Row, String)] =
      documents.map(document =>
        map(extractorClass, document, shortName,
          totalCount, successCount, failureCount)
      )(tupleRowStringEncoder)
        .persist(StorageLevel.DISK_ONLY)
        .checkpoint()

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(dataOut))

    val successResults: Dataset[Row] = mappingResults
      .filter(tuple => Option(tuple._1).isDefined)
      .map(tuple => tuple._1)(oreAggregationEncoder)

    val failures:  Array[String] = mappingResults
      .filter(tuple => Option(tuple._2).isDefined)
      .map(tuple => tuple._2).collect()

    successResults.toDF().write.avro(dataOut)

    // Summarize results
    Utils.mappingSummary(
      totalCount.value,
      successCount.value,
      failureCount.value,
      failures,
      dataOut,
      shortName,
      logger
    )

    spark.stop()

    // Clean up checkpoint directory, created above
    Utils.deleteRecursively(new File("/tmp/checkpoint"))
  }

  /**
    * Perform the mapping for a single record
    *
    * @param extractorClass Provider's extractor class
    * @param document The harvested record to map
    * @param shortName Provider short name
    * @param totalCount Accumulator to track the number of records processed
    * @param successCount Accumulator to track the number of records successfully mapped
    * @param failureCount Accumulator to track the number of records that failed to map
    * @return A tuple (Row, String)
    *           - (Row, null) on successful mapping
    *           - (null, Error message) on mapping failure
    */
  private def map(extractorClass: Class[_ <: Extractor],
                  document: String,
                  shortName: String,
                  totalCount: LongAccumulator,
                  successCount: LongAccumulator,
                  failureCount: LongAccumulator): (Row, String) = {

    totalCount.add(1)
    extractorClass.getConstructor(classOf[String], classOf[String]).newInstance(document, shortName).build() match {
      case Success(dplaMapData) =>
        successCount.add(1)
        (RowConverter.toRow(dplaMapData, model.sparkSchema), null)
      case Failure(exception) =>
        failureCount.add(1)
        (null, s"${exception.getMessage}\n" +
          s"${exception.getStackTrace.mkString("\n")}")
    }
  }
}
