package dpla.ingestion3.harvesters.pss

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._
import org.apache.spark.SparkConf

import scala.util.Try

class PssHarvester(spark: SparkSession,
                   shortName: String,
                   conf: i3Conf,
                   outputDir: String,
                   harvestLogger: Logger)
  extends Harvester(spark, shortName, conf, outputDir, harvestLogger) {

  override def mimeType: String = "application_json"

  override def localHarvest: Unit = Try{

    val endpoint = conf.harvest.endpoint
      .getOrElse(throw new RuntimeException("No endpoint specified."))

    // Run harvest.
    val harvestedData: DataFrame = spark.read
      .format("dpla.ingestion3.harvesters.pss")
      .load(endpoint)

    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L

    val finalData: DataFrame = harvestedData
      .withColumn("ingestDate", lit(unixEpoch))
      .withColumn("provider", lit(shortName))
      .withColumn("mimetype", lit(mimeType))

    // Write harvested data to file.
    finalData.write
      .format("com.databricks.spark.avro")
      .option("avroSchema", finalData.schema.toString)
      .avro(outputDir)
  }

  override def cleanUp(): Unit = Unit
}
