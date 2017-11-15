package dpla.ingestion3.harvesters.resourceSync

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import com.databricks.spark.avro._

import scala.util.Try

class RsHarvester(spark: SparkSession,
                  shortName: String,
                  conf: i3Conf,
                  outputDir: String,
                  harvestLogger: Logger)
  extends Harvester(spark, shortName, conf, outputDir, harvestLogger) {

  // TODO Do all RS enpoints support JSON?
  override protected val mimeType: String = "application_json"

  override protected def localHarvest(): Unit = ???

  override def harvest: Try[Long] = Try{

    // Set options.
    val readerOptions: Map[String, String] = Map(
      "endpoint" -> conf.harvest.endpoint
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    // Run harvest.
    val harvestedData: DataFrame = spark.read
      .format("dpla.ingestion3.harvesters.resourceSync")
      .options(readerOptions)
      .load()

    // Log errors.
    harvestedData.select("error.message", "error.errorSource.url")
      .where("error is not null")
      .collect
      .foreach(row => harvestLogger.warn("ResourceSync harvest error: " + row))

    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L

    val finalData: DataFrame = harvestedData
      .select("record.id", "record.document")
      .where("document is not null")
      .withColumn("ingestDate", lit(unixEpoch))
      .withColumn("provider", lit(shortName))
      .withColumn("mimetype", lit(mimeType))

    // Write harvested data to file.
    finalData.write
      .format("com.databricks.spark.avro")
      .option("avroSchema", finalData.schema.toString)
      .avro(outputDir)

    // Return DataFrame.
    finalData.count()

  }
}
