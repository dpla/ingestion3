package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class OaiHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf,
    harvestLogger: Logger
) extends Harvester(spark, shortName, conf, harvestLogger) {

  override def mimeType: String = "application_xml"

  override def localHarvest(): DataFrame = {
    // Set options.
    val readerOptions: Map[String, String] = Map(
      "verb" -> conf.harvest.verb,
      "metadataPrefix" -> conf.harvest.metadataPrefix,
      "harvestAllSets" -> conf.harvest.harvestAllSets,
      "setlist" -> conf.harvest.setlist,
      "blacklist" -> conf.harvest.blacklist,
      "endpoint" -> conf.harvest.endpoint,
      "removeDeleted" -> Some("true")
    ).collect { case (key, Some(value)) => key -> value } // remove None values

    // Run harvest.
    val harvestedData: DataFrame = spark.read
      .format("dpla.ingestion3.harvesters.oai.refactor")
      .options(readerOptions)
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Log errors.
    harvestedData
      .select("error.message", "error.url")
      .where("error is not null")
      .collect
      .foreach(row => {
        harvestLogger.warn(
          s"OAI harvest error ${row.getString(0)} when fetching ${row.getString(1)}"
        )
        println(row)
      })

    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L

    // return DataFrame
    harvestedData
      .select("record.id", "record.document", "record.setIds")
      .where("document is not null")
      .withColumn("ingestDate", lit(unixEpoch))
      .withColumn("provider", lit(shortName))
      .withColumn("mimetype", lit(mimeType))
  }

  override def cleanUp(): Unit = ()
}
