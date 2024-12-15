package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{Harvester, LocalHarvester}
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class OaiHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends Harvester(spark, shortName, conf) {

  override def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  override def harvest: DataFrame = {
    // Set options.
    val readerOptions: Map[String, String] = Map(
      "verb" -> conf.harvest.verb,
      "metadataPrefix" -> conf.harvest.metadataPrefix,
      "harvestAllSets" -> conf.harvest.harvestAllSets,
      "setlist" -> conf.harvest.setlist,
      "blacklist" -> conf.harvest.blacklist,
      "endpoint" -> conf.harvest.endpoint,
      "removeDeleted" -> Some("true"),
      "sleep" -> conf.harvest.sleep
    ).collect { case (key, Some(value)) => key -> value } // remove None values

    // Run harvest.
    val harvestedData: DataFrame = spark.read
      .format("dpla.ingestion3.harvesters.oai.refactor")
      .options(readerOptions)
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val logger = LogManager.getLogger(this.getClass)

    // Log errors.
    harvestedData
      .select("error.message", "error.url")
      .where("error is not null")
      .collect
      .foreach(row => {
        logger.warn(
          s"OAI harvest error ${row.getString(0)} when fetching ${row.getString(1)}"
        )
        println(row)
      })

    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L

    // return DataFrame
    harvestedData
      .select("record.id", "record.document")
      .where("document is not null")
      .withColumn("ingestDate", lit(unixEpoch))
      .withColumn("provider", lit(shortName))
      .withColumn("mimetype", lit(mimeType.toString))
  }

  override def cleanUp(): Unit = ()
}
