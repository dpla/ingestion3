package dpla.ingestion3.harvesters.pss

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

class PssHarvester(spark: SparkSession,
                   shortName: String,
                   conf: i3Conf,
                   harvestLogger: Logger)
  extends Harvester(spark, shortName, conf, harvestLogger) {

  override def mimeType: String = "application_json"

  override def localHarvest: DataFrame = {

    val endpoint = conf.harvest.endpoint
      .getOrElse(throw new RuntimeException("No endpoint specified."))

    // Run harvest.
    val harvestedData: DataFrame = spark.read
      .format("dpla.ingestion3.harvesters.pss")
      .load(endpoint)

    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L

    // Return DataFrame
    harvestedData
      .withColumn("ingestDate", lit(unixEpoch))
      .withColumn("provider", lit(shortName))
      .withColumn("mimetype", lit(mimeType))
  }

  override def cleanUp(): Unit = Unit
}
