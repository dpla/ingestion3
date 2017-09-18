package dpla.ingestion3

import dpla.ingestion3.model.{DplaMapData, ModelConverter, jsonlRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

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
  *     sbt "run-main dpla.ingestion3.JsonlEntry /input/path /output/path"
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

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val enrichedRows =
      spark.read
      .format("com.databricks.spark.avro")
      .load(inputName)


    val indexRecords: Dataset[String] = enrichedRows.map(
      row => {
        val record = ModelConverter.toModel(row)
        jsonlRecord(record)
      }
    )

    indexRecords.coalesce(1).write.text(outputName)

    sc.stop()

  }

}
