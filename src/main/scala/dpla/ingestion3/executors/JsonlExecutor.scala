package dpla.ingestion3.executors

import java.time.LocalDateTime

import dpla.ingestion3.model.{ModelConverter, jsonlRecord}
import dpla.ingestion3.dataStorage.OutputHelper
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success}

trait JsonlExecutor extends Serializable {

  /**
    * Generate JSON-L files from AVRO file
    * @param sparkConf Spark configuration
    * @param dataIn Data to transform into JSON-L
    * @param dataOut Location to save JSON-L
    */
  def executeJsonl(sparkConf: SparkConf,
                   dataIn: String,
                   dataOut: String,
                   shortName: String,
                   logger: Logger): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "jsonl", startDateTime)

    val outputPath: String = outputHelper.activityPath

    logger.info("Starting JSON-L export")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val enrichedRows =
      spark.read
        .format("com.databricks.spark.avro")
        .load(dataIn)

    val indexRecords: Dataset[String] = enrichedRows.map(
      row => {
        val record = ModelConverter.toModel(row)
        jsonlRecord(record)
      }
    )

    // This should always write out as #text() because if we use #json() then the
    // data will be written out inside a JSON object (e.g. {'value': <doc>}) which is
    // invalid for our use
    indexRecords.coalesce(1).write.text(outputPath)

    // Create and write manifest.

    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "JSON-L",
      "Provider" -> shortName,
      "Record count" -> indexRecords.count.toString,
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
