package dpla.ingestion3.executors

import dpla.ingestion3.dataStorage.OutputHelper

import java.time.LocalDateTime
import dpla.ingestion3.model.{ModelConverter, jsonlRecord}
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success}

trait JsonlExecutor extends Serializable {

  /** Generate JSON-L files from AVRO file
    * @param sparkConf
    *   Spark configuration
    * @param dataIn
    *   Data to transform into JSON-L
    * @param dataOut
    *   Location to save JSON-L
    * @param shortName
    *   Provider shortname
    */
  def executeJsonl(
      sparkConf: SparkConf,
      dataIn: String,
      dataOut: String,
      shortName: String
  ): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "jsonl", startDateTime)

    val outputPath: String = outputHelper.activityPath

    val logger = LogManager.getLogger(this.getClass)

    logger.info("Starting JSON-L export")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val enrichedRows: DataFrame = spark.read.format("avro").load(dataIn)

    val indexRecords: Dataset[String] = enrichedRows
      .map(row => {
        val record = ModelConverter.toModel(row)
        jsonlRecord(record)
      })

    // This should always write out as #text() because if we use #json() then the
    // data will be written out inside a JSON object (e.g. {'value': <doc>}) which is
    // invalid for our use
    indexRecords.write.option("compression", "gzip").text(outputPath)
    indexRecords.unpersist(false)

    val indexCount = spark.read.text(outputPath).count()

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
