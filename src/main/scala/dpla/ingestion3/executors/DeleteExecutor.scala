package dpla.ingestion3.executors

import java.time.LocalDateTime

import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.model.{ModelConverter, jsonlRecord}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success}

trait DeleteExecutor extends Serializable {

  /**

    * @param sparkConf  Spark configuration
    * @param dataIn     Data to transform into
    * @param dataOut    Location to save
    * @param deleteIds  IDs to delete
    * @param shortName  Provider shortname
    * @param logger     Logger object
    */
  def executeDelete(sparkConf: SparkConf,
                    dataIn: String,
                    dataOut: String,
                    deleteIds: String,
                    shortName: String,
                    logger: Logger): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    // Output for this process in new jsonl sans deleteIds
    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "jsonl", startDateTime)

    val outputPath: String = outputHelper.activityPath

    logger.info("Starting delete")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val enrichedRows: DataFrame = spark.read.format("avro").load(dataIn)

    // delete items
    val deleteUris = deleteIds.split(",").map(id => s"http://dp.la/api/items/$id")

    logger.info(s"Sourced enrichment data from $dataIn")
    logger.info(s"Deleting ${deleteUris.length} IDs from enriched data")
    logger.info(s"Saving to $outputPath")

    val indexRecords: Dataset[String] = enrichedRows
      .filter(row => !deleteUris.contains(row.getString(0))) // filter out rows where dplaUri matches
      .map(row => {
        val record = ModelConverter.toModel(row)
        jsonlRecord(record)
      })
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val indexCount = indexRecords.count

    logger.info(s"Saved $indexCount to JSONL export")
    // This should always write out as #text() because if we use #json() then the
    // data will be written out inside a JSON object (e.g. {'value': <doc>}) which is
    // invalid for our use
    indexRecords.write.text(outputPath)

    // Create and write manifest.

    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "JSON-L",
      "Provider" -> shortName,
      "Record count" -> indexCount.toString,
      "Input" -> dataIn
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    sc.stop()

    logger.info("JSON-L export complete")

    // Return output path of jsonl files.
    outputPath
  }
}
