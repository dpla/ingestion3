package dpla.ingestion3.entries.ingest

import java.io.File

import dpla.ingestion3.model.{ModelConverter, jsonlRecord}
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.log4j.Logger

/**
  * Driver for reading DplaMapData records (mapped or enriched) and generating
  * JSONL text, which can be bulk loaded into a DPLA Ingestion1 index, in
  * Elasticsearch 0.90
  *
  * Arguments:
  *   1) The path or URL to the mapped / enriched data (file, directory, or s3
  *      URI)
  *   2) The path or URL to the output (directory or s3 "folder")
  *
  * The output directory will contain one file named "part-*" that constitutes
  * the JSONL.
  *
  *   Usage
  *   -----
  *   To invoke via sbt:
  *     sbt "run-main dpla.ingestion3.entries.JsonlEntry /input/path /output/path"
  *
  */
object JsonlEntry {

  def main(args: Array[String]): Unit = {

    val inputName = args(0)
    val outputName = args(1)

    val sparkConf =
      new SparkConf()
      .setAppName("jsonl")
      .setMaster("local[*]")


    executeJsonL(sparkConf, inputName, outputName, Utils.createLogger("jsonl", ""))
  }

  /**
    * Generate JSON-L files from AVRO file
    * @param sparkConf Spark configuration
    * @param dataIn Data to transform into JSON-L
    * @param dataOut Location to save JSON-L
    */
  def executeJsonL(sparkConf: SparkConf, dataIn: String, dataOut: String, logger: Logger) = {
    logger.info("Starting JSON-L export")

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(dataOut))

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
    indexRecords.coalesce(1).write.text(dataOut)
    sc.stop()

    logger.info("JSON-L export complete")
  }
}
