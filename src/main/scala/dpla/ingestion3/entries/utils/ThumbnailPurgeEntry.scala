package dpla.ingestion3.entries.utils

import dpla.ingestion3.executors.ThumbnailPurgeExecutor
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ThumbnailPurgeEntry {

  /** Entry to delete all thumbnails for a provider
    *
    * @param args
    *   Path to mapped data S3 bucket for thumbnails
    */
  def main(args: Array[String]): Unit = {

    if (args.length.!=(2)) {
      println(
        "Not enough args. Expecting: " +
          "(1) path to mapped data" +
          "(2) thumbnail bucket"
      )
      return
    }

    val inpath = args(0)
    val bucket = args(1)

    val conf: SparkConf = new SparkConf().setAppName("Purge Thumbnails")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    ThumbnailPurgeExecutor.execute(spark, inpath, bucket)

    spark.stop()
  }
}
