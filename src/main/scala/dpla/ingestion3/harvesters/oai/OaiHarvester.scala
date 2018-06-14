package dpla.ingestion3.harvesters.oai

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.util.Try

class OaiHarvester(shortName: String,
                   conf: i3Conf,
                   outputDir: String,
                   harvestLogger: Logger)
  extends Harvester(shortName, conf, outputDir, harvestLogger) {

  override protected val mimeType: String = "application_xml"

  override protected def localHarvest(): Unit = ???

  override protected def runHarvest: Try[DataFrame] = Try{
    // Set options.
    val readerOptions: Map[String, String] = Map(
      "verb" -> conf.harvest.verb,
      "metadataPrefix" -> conf.harvest.metadataPrefix,
      "harvestAllSets" -> conf.harvest.harvestAllSets,
      "setlist" -> conf.harvest.setlist,
      "blacklist" -> conf.harvest.blacklist,
      "endpoint" -> conf.harvest.endpoint,
      "removeDeleted" -> Some("true")
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    // Run harvest.
    val harvestedData: DataFrame = spark.read
      .format("dpla.ingestion3.harvesters.oai.refactor")
      .options(readerOptions)
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Log errors.
    harvestedData.select("error.message", "error.url")
      .where("error is not null")
      .collect
      .foreach(row => harvestLogger.warn(s"OAI harvest error ${row.getString(0)} when fetching ${row.getString(1)}" ))

    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L

    val finalData: DataFrame = harvestedData
      .select("record.id", "record.document")
      .where("document is not null")
      .withColumn("ingestDate", lit(unixEpoch))
      .withColumn("provider", lit(shortName))
      .withColumn("mimetype", lit(mimeType))

    // Write harvested data to file.
    finalData
      .write
      .format("com.databricks.spark.avro")
      .option("avroSchema", finalData.schema.toString)
      .avro(outputDir)

    finalData
  }
}
