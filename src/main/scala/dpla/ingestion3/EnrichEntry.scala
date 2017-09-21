package dpla.ingestion3

import java.io.File

import dpla.ingestion3.enrichments.{EnrichmentDriver, Twofisher}
import dpla.ingestion3.model.{DplaMapData, ModelConverter, RowConverter}
import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import com.databricks.spark.avro._
import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

/**
  * Expects three parameters:
  *   1) a path to the harvested data
  *   2) a path to output the mapped data
  *   3) a path to the application configuration file
  *
  *   Usage
  *   -----
  *   To invoke via sbt:
  *     sbt "run-main dpla.ingestion3.EnrichEntry
  *     --input=/input/path/to/mapped.avro
  *     --output=/output/path/to/enriched.avro
  *     --conf=/path/to/application.conf
  *
  */

object EnrichEntry {

  def main(args: Array[String]): Unit = {

    // Read in command line args
    val cmdArgs = new CmdArgs(args)

    val inputDir = cmdArgs.input.getOrElse(throw new IllegalArgumentException("Missing input dir"))
    val outputDir = cmdArgs.output.getOrElse(throw new IllegalArgumentException("Missing output dir"))
    val confFile = cmdArgs.configFile.getOrElse(throw new IllegalArgumentException("Missing conf file"))

    val logger = LogManager.getLogger(EnrichEntry.getClass)

    // Load configuration from file
    val i3Conf = new Ingestion3Conf(confFile).load()

    val sparkConf = new SparkConf()
      .setAppName("Enrichment")
      .setMaster(i3Conf.spark.sparkMaster.getOrElse("local[*]"))

    implicit val dplaMapDataEncoder = org.apache.spark.sql.Encoders.kryo[DplaMapData]

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._
    val dplaMapDataRowEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    // Load the mapped records
    val mappedRows = spark.read.avro(inputDir)

    // Run the enrichments over the Dataframe
    val enrichedRows = mappedRows.map(
      row => {
        val dplaMapData = ModelConverter.toModel(row)
        val enrichedDplaMapData = new EnrichmentDriver(i3Conf).enrich(dplaMapData)
        RowConverter.toRow(enrichedDplaMapData, model.sparkSchema)
      }
    )(dplaMapDataRowEncoder)

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(outputDir))

    // Save mapped records out to Avro file
    enrichedRows.toDF().write
      .format("com.databricks.spark.avro")
      .save(outputDir)


    // Gather some stats
    val mappedRecordCount = mappedRows.count()
    val enrichedRecordCount = enrichedRows.count()

    sc.stop()

    logger.debug(s"Mapped ${mappedRecordCount} records and enriched ${enrichedRecordCount} records")
    logger.debug(s"${mappedRecordCount-enrichedRecordCount} enrichment errors")
  }
}
