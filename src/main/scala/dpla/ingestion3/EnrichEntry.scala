package dpla.ingestion3

import java.io.File

import dpla.ingestion3.enrichments.EnrichmentDriver
import dpla.ingestion3.model.{DplaMapData, ModelConverter, RowConverter}
import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import com.databricks.spark.avro._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

/**
  * Expects two parameters:
  *   1) a path to the harvested data
  *   2) a path to output the mapped data
  *
  *   Usage
  *   -----
  *   To invoke via sbt:
  *     sbt "run-main dpla.ingestion3.EnrichEntry /input/path/to/mapped.avro /output/path/to/enriched.avro"
  *
  */

object EnrichEntry {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(EnrichEntry.getClass)

    if (args.length != 2)
      logger.error("Incorrect number of parameters provided. Expected <input> <output>")

    // Get files
    val dataIn = args(0)
    val dataOut = args(1)

    val sparkConf = new SparkConf()
      .setAppName("Enrichment")
      // TODO there should be a central place to store the sparkMaster
      .setMaster("local[*]")

    implicit val dplaMapDataEncoder = org.apache.spark.sql.Encoders.kryo[DplaMapData]

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._
    val dplaMapDataRowEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)

    // Load the mapped records
    val mappedRows = spark.read.avro(dataIn)

    // Run the enrichments over the Dataframe
    val enrichedRows = mappedRows.map(
      row => {
        val dplaMapData = ModelConverter.toModel(row)
        val enrichedDplaMapData = new EnrichmentDriver().enrich(dplaMapData)
        RowConverter.toRow(enrichedDplaMapData, model.sparkSchema)
      }
    )(dplaMapDataRowEncoder)

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(dataOut))

    // Save mapped records out to Avro file
    enrichedRows.toDF().write
      .format("com.databricks.spark.avro")
      .save(dataOut)


    // Gather some stats
    val mappedRecordCount = mappedRows.count()
    val enrichedRecordCount = enrichedRows.count()

    sc.stop()

    logger.debug(s"Mapped ${mappedRecordCount} records and enriched ${enrichedRecordCount} records")
    logger.debug(s"${mappedRecordCount-enrichedRecordCount} enrichment errors")
  }
}
