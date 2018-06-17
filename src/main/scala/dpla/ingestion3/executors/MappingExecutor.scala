package dpla.ingestion3.executors

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

import com.databricks.spark.avro._
import dpla.ingestion3.messages.{MappingSummary, MappingSummaryData, MessageProcessor, Tabulator}
import dpla.ingestion3.model
import dpla.ingestion3.model.RowConverter
import dpla.ingestion3.utils.{ProviderRegistry, Utils}
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

    logger.info("Mapping started")
    val spark = SparkSession.builder()
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
    val documents: Dataset[String] = harvestedRecords.select("document").as[String].limit(50)

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

    // Begin new error handling
    import org.apache.spark.sql.functions.explode

    val messages = MessageProcessor.getAllMessages(successResults)

    val messagesExploded = messages
      .withColumn("level", explode($"level")) // weird syntax errors in other files with $
      .withColumn("message", explode($"message"))
      .withColumn("field", explode($"field"))
      .withColumn("value", explode($"value"))
      .withColumn("id", explode($"id"))
      .distinct() // I ended up with qaudruplication of all messages. I suspect `explode` but the dataset might be recomputed..no better option atm.

    val warnings = MessageProcessor.getWarnings(messagesExploded)
    val errors = MessageProcessor.getErrors(messagesExploded)

    // get counts
    val attemptedCount = successResults.count()
    val validCount = successResults.select("dplaUri").where("size(messages) == 0").count()
    val warnCount = warnings.count()
    val errorCount = errors.count()

    val recordErrorCount = MessageProcessor.getDistinctIdCount(errors)
    val recordWarnCount = MessageProcessor.getDistinctIdCount(warnings)


    // Make a table
    val sumTable = List(List("Status", "Count"),
                        List("Attempted", Utils.formatNumber(attemptedCount)),
                        List("Valid", Utils.formatNumber(validCount)),
                        List("Warning", ""),
                        List("- Messages", Utils.formatNumber(warnCount)),
                        List("- Records", Utils.formatNumber(recordWarnCount)),
                        List("Error", ""),
                        List("- Messages", Utils.formatNumber(errorCount)),
                        List("- Records", Utils.formatNumber(recordErrorCount)))
    // format and log the table
//    val formattedTable = Tabulator.format(sumTable)
//    logger.info("\n" + formattedTable) // new line pad to get everything on the same line

    // TODO -- Move this off to the MappingSummary generator (it should accept a long)
    val instant = Instant.ofEpochMilli(System.currentTimeMillis())
    val dtUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("America/New_York"))
    val dtFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")
    val dateTime = dtFormatter.format(dtUtc)

    val mappingSummary = MappingSummaryData(
      shortName,
      dateTime,
      attemptedCount,
      validCount,
      warnCount,
      errorCount,
      recordWarnCount,
      recordErrorCount)
    logger.info(MappingSummary.getSummary(mappingSummary))

    // TODO Relocate this log code
    // Write warn and error messages to CSV files. These should all share the same timestamp, minor work TBD
    val baseLogDir = s"$dataOut/../logs/"
    val time = System.currentTimeMillis().toString
    val logList = List("all" -> messagesExploded, "error" -> errors, "warn" -> warnings)
    def getSpecificLogDir(name: String, time: String) = s"$shortName-$time-map-$name"

    logList.foreach { case (name: String, data: Dataset[Row]) =>
      val path = baseLogDir + getSpecificLogDir(name, time)
      Utils.writeLogs(path, name, data, shortName)
      logger.info(s"Saved ${name.toUpperCase} log to: ${new File(path).getCanonicalPath}")
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
    // null == no error message (string)
    (RowConverter.toRow(mappedDocument, model.sparkSchema), null)

  }
}