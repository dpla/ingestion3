package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import org.apache.spark.sql.DataFrame
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import com.databricks.spark.avro._

import scala.util.Try

class OaiHarvester(shortName: String,
                   conf: i3Conf,
                   outputDir: String,
                   harvestLogger: Logger)
  extends Harvester(shortName, conf, outputDir, harvestLogger) {

  override protected val mimeType: String = "application_xml"

  override protected def runHarvest: Try[DataFrame] = Try{

    // Set options.
    val readerOptions: Map[String, String] = Map(
      "verb" -> conf.harvest.verb,
      "metadataPrefix" -> conf.harvest.metadataPrefix,
      "harvestAllSets" -> conf.harvest.harvestAllSets,
      "setlist" -> conf.harvest.setlist,
      "blacklist" -> conf.harvest.blacklist,
      "endpoint" -> conf.harvest.endpoint
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    // Run harvest.
    val harvestedData: DataFrame = spark.read
      .format("dpla.ingestion3.harvesters.oai")
      .options(readerOptions)
      .load()

    // Log errors.
    harvestedData.select("error.message", "error.errorSource.url")
      .where("error is not null")
      .collect
      .foreach(row => harvestLogger.warn("OAI harvest error: " + row))

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
    finalData
  }
}
