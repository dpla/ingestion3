package dpla.ingestion3.executors

import dpla.ingestion3.utils.S3FileHelper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Executes the deletion of thumbnails from the specified bucket
  */
object ThumbnailPurgeExecutor extends S3FileHelper {

  private lazy val dplaUriPrefix = "http://dp.la/api/items/"

  def execute(spark: SparkSession, input: String, bucket: String): Unit = {

    val df = spark.read.format("avro").load(input)

    df.createOrReplaceTempView("df")

    val thumbnailKeys = spark
      .sql("SELECT df.dplaUri from df")
      .withColumn("dplaId", regexp_replace(col("dplaUri"), dplaUriPrefix, ""))
      .select("dplaId")
      .collect()
      .map(row => thumbnailKey(row.getString(0)))

    deleteS3Keys(bucket, thumbnailKeys)
  }

  /** Build key for thumbnail from DPLA ID
    *
    * @param id
    *   DPLA ID
    * @return
    *   S3 object key
    */
  def thumbnailKey(id: String): String =
    s"${id(0)}/${id(1)}/${id(2)}/${id(3)}/$id.jpg"
}
