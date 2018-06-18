package dpla.ingestion3.executors

import java.io.File

import com.databricks.spark.avro._
import dpla.ingestion3.messages.{MappingSummary, MappingSummaryData, MessageProcessor}
import dpla.ingestion3.model
import dpla.ingestion3.model.RowConverter
import dpla.ingestion3.utils.{ProviderRegistry, Utils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.functions.explode
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

    logger.info(s"${shortName.toUpperCase} mapping started")

    // @michael Any issues with making SparkSession implicit?
    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", false)
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

    // Run the mapping over the Dataframe
    val documents: Dataset[String] = harvestedRecords.select("document").as[String]

    val dplaMap = new DplaMap()

    val mappingResults: Dataset[(Row, String)] =
      documents.map(document =>
        dplaMap.map(document, shortName,
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

    // Begin new error and message handling
    val messages = MessageProcessor.getAllMessages(successResults)

    val messagesExploded = messages
      .withColumn("level", explode($"level"))
      .withColumn("message", explode($"message"))
      .withColumn("field", explode($"field"))
      .withColumn("value", explode($"value"))
      .withColumn("id", explode($"id"))
      .distinct()
    // Calling distinct here because I was ending up with qaudruplication of all messages.
    // I suspect `explode` but the dataset might be recomputed unnecessarily

    val warnings = MessageProcessor.getWarnings(messagesExploded)
    val errors = MessageProcessor.getErrors(messagesExploded)

    // get counts
    val attemptedCount = mappingResults.count() // successResults.count()
    val validCount = successResults.select("dplaUri").where("size(messages) == 0").count()
    val warnCount = warnings.count()
    val errorCount = errors.count()

    val recordErrorCount = MessageProcessor.getDistinctIdCount(errors)
    val recordWarnCount = MessageProcessor.getDistinctIdCount(warnings)

    val errorMsgDets = MessageProcessor.getMessageFieldSummary(errors).mkString("\n")
    val warnMsgDets = MessageProcessor.getMessageFieldSummary(warnings).mkString("\n")

    val timeInMs = System.currentTimeMillis()
    val dateTime = Utils.formatDateTime(timeInMs)

    logger.info(s"Message summary")
    val mappingSummary = MappingSummaryData(
      shortName,
      dateTime,
      attemptedCount,
      validCount,
      warnCount,
      errorCount,
      recordWarnCount,
      recordErrorCount,
      errorMsgDets,
      warnMsgDets
    )

    logger.info(MappingSummary.getSummary(mappingSummary))
    logger.info(s"Number of exceptions ${failures.length}")

    // Write warn and error messages to CSV files. These should all share the same timestamp, minor work TBD
    val baseLogDir = s"$dataOut/../logs/"
    val time = timeInMs.toString

    val f = sc.parallelize(failures).toDS().map(r => Row(r))

    val logFileList = List("all" -> messagesExploded, "error" -> errors, "warn" -> warnings, "exceptions" -> f)
      .filter { case  (_, data: Dataset[Row]) => data.count() > 0 }

    logFileList.foreach {
      case (name: String, data: Dataset[Row]) => {
        val path = baseLogDir + s"$shortName-$time-map-$name"
        Utils.writeLogsAsCsv(path, name, data, shortName)
        logger.info(s"Saved ${name.toUpperCase} log to: ${new File(path).getCanonicalPath}")
      }
    }

    successResults.toDF().write.avro(dataOut)

    spark.stop()

    // Clean up checkpoint directory, created above
    Utils.deleteRecursively(new File("/tmp/checkpoint"))
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

    val mappedDocument = extractorClass.performMapping(document)
    (RowConverter.toRow(mappedDocument, model.sparkSchema), null)
  }
}