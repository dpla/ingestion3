package dpla.ingestion3.entries.ingest

import java.io.File

import dpla.ingestion3.executors.JsonlExecutor
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf

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
object JsonlEntry extends JsonlExecutor {

  def main(args: Array[String]): Unit = {

    val inputName = args(0)
    val outputName = args(1)

    val sparkConf =
      new SparkConf()
      .setAppName("jsonl")
      .setMaster("local[*]")

    Utils.deleteRecursively(new File(outputName))

    executeJsonl(sparkConf, inputName, outputName, Utils.createLogger("jsonl"))
  }
}
